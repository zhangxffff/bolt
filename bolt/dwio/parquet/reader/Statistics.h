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

#pragma once

#include "bolt/dwio/common/Statistics.h"
#include "bolt/dwio/parquet/thrift/codegen/parquet_types.h"

#include <cstring>
#include <optional>
namespace bytedance::bolt {
class Type;
}
namespace bytedance::bolt::dwio::common {
class ColumnStatistics;
}
namespace bytedance::bolt::parquet {

// TODO: provide function to merge multiple Statistics into one

template <typename T>
inline const T load(const char* ptr) {
  T ret;
  std::memcpy(&ret, ptr, sizeof(ret));
  return ret;
}

template <typename T>
inline std::optional<T> getMin(const thrift::Statistics& columnChunkStats) {
  return columnChunkStats.__isset.min_value
      ? load<T>(columnChunkStats.min_value.data())
      : (columnChunkStats.__isset.min
             ? std::optional<T>(load<T>(columnChunkStats.min.data()))
             : std::nullopt);
}

template <typename T>
inline std::optional<T> getMax(const thrift::Statistics& columnChunkStats) {
  return columnChunkStats.__isset.max_value
      ? std::optional<T>(load<T>(columnChunkStats.max_value.data()))
      : (columnChunkStats.__isset.max
             ? std::optional<T>(load<T>(columnChunkStats.max.data()))
             : std::nullopt);
}

template <>
inline std::optional<std::string> getMin(
    const thrift::Statistics& columnChunkStats) {
  return columnChunkStats.__isset.min_value
      ? std::optional(columnChunkStats.min_value)
      : (columnChunkStats.__isset.min ? std::optional(columnChunkStats.min)
                                      : std::nullopt);
}

template <>
inline std::optional<std::string> getMax(
    const thrift::Statistics& columnChunkStats) {
  return columnChunkStats.__isset.max_value
      ? std::optional(columnChunkStats.max_value)
      : (columnChunkStats.__isset.max ? std::optional(columnChunkStats.max)
                                      : std::nullopt);
}

std::unique_ptr<dwio::common::ColumnStatistics> buildColumnStatisticsFromThrift(
    const thrift::Statistics& columnChunkStats,
    std::unique_ptr<BlockSplitBloomFilter> blockBloom,
    std::vector<std::pair<
        std::unique_ptr<struct NgramTokenExtractor>,
        std::unique_ptr<NGramBloomFilter>>> nGramStats,
    const bolt::Type& type,
    uint64_t numRowsInRowGroup,
    bool isUnsignedInteger);

} // namespace bytedance::bolt::parquet
