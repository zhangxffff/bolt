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

#include "bolt/common/process/ProcessBase.h"

#include <limits.h>
#include <stdlib.h>
#include <sys/utsname.h>
#include <time.h>
#include <unistd.h>

#include <fstream>
#include <iomanip>
#include <set>
#include <sstream>

#include <folly/CpuId.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

constexpr const char* kProcSelfCmdline = "/proc/self/cmdline";

DECLARE_bool(avx2); // Enables use of AVX2 when available NOLINT

DECLARE_bool(bmi2); // Enables use of BMI2 when available NOLINT
namespace bytedance {
namespace bolt {
namespace process {

/**
 * Current executable's name.
 */
std::string getAppName() {
  const char* result = getenv("_");
  if (result) {
    return result;
  }

  // if we're running under gtest, getenv will return null
  std::string appName;
  if (folly::readFile(kProcSelfCmdline, appName)) {
    auto pos = appName.find('\0');
    if (pos != std::string::npos) {
      appName = appName.substr(0, pos);
    }

    return appName;
  }

  return "";
}

/**
 * This machine's name.
 */
std::string getHostName() {
  char hostbuf[_POSIX_HOST_NAME_MAX + 1];
  if (gethostname(hostbuf, _POSIX_HOST_NAME_MAX + 1) < 0) {
    return "";
  } else {
    // When the host name is precisely HOST_NAME_MAX bytes long, gethostname
    // returns 0 even though the result is not NUL-terminated. Manually NUL-
    // terminate to handle that case.
    hostbuf[_POSIX_HOST_NAME_MAX] = '\0';
    return hostbuf;
  }
}

/**
 * Process identifier.
 */
pid_t getProcessId() {
  return getpid();
}

/**
 * Current thread's identifier.
 */
pthread_t getThreadId() {
  return pthread_self();
}

/**
 * Get current working directory.
 */
std::string getCurrentDirectory() {
  char buf[PATH_MAX];
  return getcwd(buf, PATH_MAX);
}

uint64_t threadCpuNanos() {
  timespec ts;
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
  return ts.tv_sec * 1'000'000'000 + ts.tv_nsec;
}

namespace {
bool bmi2CpuFlag = folly::CpuId().bmi2();
bool avx2CpuFlag = folly::CpuId().avx2();
} // namespace

bool hasAvx2() {
#ifdef __AVX2__
  return avx2CpuFlag && FLAGS_avx2;
#else
  return false;
#endif
}

bool hasBmi2() {
#ifdef __BMI2__
  return bmi2CpuFlag && FLAGS_bmi2;
#else
  return false;
#endif
}

bool hasNeon() {
#if (defined(__ARM_NEON) || defined(__ARM_NEON__)) && !defined(__CUDACC__)
  return true;
#else
  return false;
#endif
}

namespace {
// Helper to format bytes into a human-readable string (e.g. "15.6 GB").
std::string formatBytes(int64_t bytes) {
  const char* units[] = {"B", "KB", "MB", "GB", "TB"};
  double value = static_cast<double>(bytes);
  int unitIndex = 0;
  while (value >= 1024.0 && unitIndex < 4) {
    value /= 1024.0;
    ++unitIndex;
  }
  std::ostringstream oss;
  if (unitIndex == 0) {
    oss << bytes << " B";
  } else {
    oss << std::fixed << std::setprecision(2) << value << " " << units[unitIndex];
  }
  return oss.str();
}
} // namespace

std::string CpuInfo::toString() const {
  std::ostringstream oss;
  oss << "CPU Model        : " << modelName << "\n";
  oss << "Physical Cores   : " << physicalCores << "\n";
  oss << "Logical Cores    : " << logicalCores << "\n";
  oss << "Architecture     : " << architecture << "\n";
  return oss.str();
}

std::string MemoryInfo::toString() const {
  std::ostringstream oss;
  oss << "Total Memory     : " << formatBytes(totalMemoryBytes) << "\n";
  oss << "Available Memory : " << formatBytes(availableMemoryBytes) << "\n";
  oss << "Free Memory      : " << formatBytes(freeMemoryBytes) << "\n";
  oss << "Process RSS      : " << formatBytes(processRssBytes) << "\n";
  return oss.str();
}

CpuInfo getCpuInfo() {
  CpuInfo info;

  // Get architecture from uname.
  struct utsname unameData {};
  if (uname(&unameData) == 0) {
    info.architecture = unameData.machine;
  }

  // Get logical core count from sysconf.
  long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  if (nprocs > 0) {
    info.logicalCores = static_cast<int>(nprocs);
  }

  // Parse /proc/cpuinfo for model name and physical core count.
  std::ifstream cpuinfo("/proc/cpuinfo");
  if (cpuinfo) {
    std::string line;
    std::set<std::string> physicalIds;
    std::set<std::string> coreKeys; // "physical_id:core_id" pairs
    while (std::getline(cpuinfo, line)) {
      if (info.modelName.empty() && line.find("model name") == 0) {
        auto pos = line.find(':');
        if (pos != std::string::npos) {
          info.modelName = line.substr(pos + 2);
        }
      }
    }
  }

  // Use sysconf for physical core count on systems where /proc/cpuinfo
  // parsing may not yield physical IDs (e.g. containers).
  if (info.physicalCores == 0) {
    info.physicalCores = info.logicalCores;
  }

  return info;
}

MemoryInfo getMemoryInfo() {
  MemoryInfo info;

  // Parse /proc/meminfo for total, available, and free memory.
  std::ifstream meminfo("/proc/meminfo");
  if (meminfo) {
    std::string line;
    while (std::getline(meminfo, line)) {
      int64_t valueKb = 0;
      if (line.find("MemTotal:") == 0) {
        std::istringstream iss(line.substr(9));
        iss >> valueKb;
        info.totalMemoryBytes = valueKb * 1024;
      } else if (line.find("MemAvailable:") == 0) {
        std::istringstream iss(line.substr(13));
        iss >> valueKb;
        info.availableMemoryBytes = valueKb * 1024;
      } else if (line.find("MemFree:") == 0) {
        std::istringstream iss(line.substr(8));
        iss >> valueKb;
        info.freeMemoryBytes = valueKb * 1024;
      }
    }
  }

  // Get process RSS from /proc/self/statm.
  static const int64_t kPageSize = [] {
    const long pageSize = sysconf(_SC_PAGESIZE);
    return pageSize > 0 ? pageSize : 4096;
  }();

  std::ifstream statmFile("/proc/self/statm");
  if (statmFile) {
    int64_t vmPages = 0;
    int64_t rssPages = 0;
    statmFile >> vmPages >> rssPages;
    info.processRssBytes = rssPages * kPageSize;
  }

  return info;
}

void logSystemInfo() {
  auto cpuInfo = getCpuInfo();
  auto memInfo = getMemoryInfo();

  std::ostringstream oss;
  oss << "\n========================================\n";
  oss << "        System Information\n";
  oss << "========================================\n";
  oss << cpuInfo.toString();
  oss << "----------------------------------------\n";
  oss << memInfo.toString();
  oss << "========================================\n";

  LOG(INFO) << oss.str();
}

} // namespace process
} // namespace bolt
} // namespace bytedance
