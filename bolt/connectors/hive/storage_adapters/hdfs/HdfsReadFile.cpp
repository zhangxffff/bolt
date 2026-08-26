/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * --------------------------------------------------------------------------
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file has been modified by ByteDance Ltd. and/or its affiliates on
 * 2025-11-11.
 *
 * Original file was released under the Apache License 2.0,
 * with the full license text available at:
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This modified file is released under the same license.
 * --------------------------------------------------------------------------
 */

#include "HdfsReadFile.h"

#include <chrono>
#include <mutex>
#include <random>
#include <thread>

#include "bolt/common/flags/BoltFlags.h"
#include "bolt/external/hdfs/ArrowHdfsInternal.h"
namespace bytedance::bolt {

struct HdfsFile {
  filesystems::arrow::io::internal::LibHdfsShim* driver_;
  hdfsFS client_;
  hdfsFile handle_;

  HdfsFile() : driver_(nullptr), client_(nullptr), handle_(nullptr) {}
  ~HdfsFile() {
    if (handle_ && driver_->CloseFile(client_, handle_) == -1) {
      LOG(ERROR) << "Unable to close file, errno: " << errno;
    }
  }

  void open(
      filesystems::arrow::io::internal::LibHdfsShim* driver,
      hdfsFS client,
      const std::string& path,
      int bufferSize) {
    driver_ = driver;
    client_ = client;
    handle_ = driver->OpenFile(client, path.data(), O_RDONLY, bufferSize, 0, 0);
    BOLT_CHECK_NOT_NULL(
        handle_,
        "Unable to open file {}. got error: {}",
        path,
        driver_->GetLastExceptionRootCause());
  }

  void seek(uint64_t offset) const {
    BOLT_CHECK_EQ(
        driver_->Seek(client_, handle_, offset),
        0,
        "Cannot seek through HDFS file, error is : {}",
        driver_->GetLastExceptionRootCause());
  }

  int32_t read(char* pos, uint64_t length) const {
    auto bytesRead = driver_->Read(client_, handle_, pos, length);
    BOLT_CHECK(bytesRead >= 0, "Read failure in HDFSReadFile::preadInternal.");
    return bytesRead;
  }
};

class HdfsReadFile::Impl {
 public:
  Impl(
      filesystems::arrow::io::internal::LibHdfsShim* driver,
      hdfsFS hdfs,
      const std::string_view path,
      int bufferSize)
      : driver_(driver),
        hdfsClient_(hdfs),
        filePath_(path),
        bufferSize_(bufferSize) {
    fileInfo_ = driver_->GetPathInfo(hdfsClient_, filePath_.data());
    if (fileInfo_ == nullptr) {
      auto error = fmt::format(
          "FileNotFoundException: Path {} does not exist.", filePath_);
      auto errMsg = fmt::format(
          "Unable to get file path info for file: {}. got error: {}",
          filePath_,
          error);
      if (error.find("FileNotFoundException") != std::string::npos) {
        BOLT_FILE_NOT_FOUND_ERROR(errMsg);
      }
      BOLT_FAIL(errMsg);
    }
  }

  ~Impl() {
    // Should call hdfsFreeFileInfo to avoid memory leak
    if (fileInfo_) {
      driver_->FreeFileInfo(fileInfo_, 1);
    }
  }

  void preadInternal(uint64_t offset, uint64_t length, char* pos) const {
    injectFault(offset, length);
    checkFileReadParameters(offset, length);
    if (!file_->handle_) {
      file_->open(driver_, hdfsClient_, filePath_, bufferSize_);
    }
    file_->seek(offset);
    uint64_t totalBytesRead = 0;
    while (totalBytesRead < length) {
      auto bytesRead = file_->read(pos, length - totalBytesRead);
      totalBytesRead += bytesRead;
      pos += bytesRead;
    }
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buf) const {
    preadInternal(offset, length, static_cast<char*>(buf));
    return {static_cast<char*>(buf), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const {
    std::string result(length, 0);
    char* pos = result.data();
    preadInternal(offset, length, pos);
    return result;
  }

  uint64_t size() const {
    return fileInfo_->mSize;
  }

  uint64_t memoryUsage() const {
    return fileInfo_->mBlockSize;
  }

  bool shouldCoalesce() const {
    return false;
  }

  std::string getName() const {
    return filePath_;
  }

  // Stalls and fails a fraction of reads. On by default in this build.
  // Every HDFS read - synchronous and async split preload alike - funnels
  // through preadInternal, so this single point covers both.
  void injectFault(uint64_t offset, uint64_t length) const {
    const auto delayMs = FLAGS_bolt_testing_hdfs_read_delay_ms;
    const auto failurePct = FLAGS_bolt_testing_hdfs_read_failure_pct;
    if (delayMs > 0 || failurePct > 0) {
      // Announce once per process. This build is not safe for real workloads
      // and should be impossible to mistake for one.
      static std::once_flag announced;
      std::call_once(announced, [delayMs, failurePct]() {
        LOG(ERROR) << "HDFS FAULT INJECTION BUILD: every read stalls " << delayMs
                   << "ms and " << failurePct
                   << "% of reads fail. Do not run real workloads on this build.";
      });
    }
    if (delayMs > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
    }

    if (failurePct <= 0) {
      return;
    }
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int32_t> dist(1, 100);
    if (dist(rng) > failurePct) {
      return;
    }
    // Deliberately not phrased as IOException/BlockMissingException: those
    // match the corrupt-file whitelist and would make the reader skip the file
    // instead of failing the task.
    BOLT_FAIL(
        "Fault injection: simulated HDFS read failure on {} at offset {} length {}",
        filePath_,
        offset,
        length);
  }

  void checkFileReadParameters(uint64_t offset, uint64_t length) const {
    auto fileSize = size();
    auto endPoint = offset + length;
    BOLT_CHECK_GE(
        fileSize,
        endPoint,
        "Cannot read HDFS file beyond its size: {}, offset: {}, end point: {}",
        fileSize,
        offset,
        endPoint);
  }

 private:
  filesystems::arrow::io::internal::LibHdfsShim* driver_;
  hdfsFS hdfsClient_;
  std::string filePath_;
  int bufferSize_;
  hdfsFileInfo* fileInfo_;
  folly::ThreadLocal<HdfsFile> file_;
};

HdfsReadFile::HdfsReadFile(
    filesystems::arrow::io::internal::LibHdfsShim* driver,
    hdfsFS hdfs,
    const std::string_view path,
    int bufferSize)
    : pImpl(std::make_unique<Impl>(driver, hdfs, path, bufferSize)) {}

HdfsReadFile::~HdfsReadFile() = default;

std::string_view
HdfsReadFile::pread(uint64_t offset, uint64_t length, void* buf) const {
  return pImpl->pread(offset, length, buf);
}

std::string HdfsReadFile::pread(uint64_t offset, uint64_t length) const {
  return pImpl->pread(offset, length);
}

uint64_t HdfsReadFile::size() const {
  return pImpl->size();
}

uint64_t HdfsReadFile::memoryUsage() const {
  return pImpl->memoryUsage();
}

bool HdfsReadFile::shouldCoalesce() const {
  return pImpl->shouldCoalesce();
}

std::string HdfsReadFile::getName() const {
  return pImpl->getName();
}

void HdfsReadFile::checkFileReadParameters(uint64_t offset, uint64_t length)
    const {
  pImpl->checkFileReadParameters(offset, length);
}

} // namespace bytedance::bolt
