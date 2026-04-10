/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
 * SPDX-License-Identifier: Apache-2.0
 */

#include "bolt/common/process/ProcessBase.h"

#include <gtest/gtest.h>

using namespace bytedance::bolt::process;

TEST(ProcessBaseTest, getCpuInfo) {
  auto info = getCpuInfo();
  EXPECT_GT(info.logicalCores, 0);
  EXPECT_GT(info.physicalCores, 0);
  EXPECT_FALSE(info.architecture.empty());
  EXPECT_FALSE(info.toString().empty());
}

TEST(ProcessBaseTest, getMemoryInfo) {
  auto info = getMemoryInfo();
  EXPECT_GT(info.totalMemoryBytes, 0);
  EXPECT_GT(info.processRssBytes, 0);
  EXPECT_FALSE(info.toString().empty());
}

TEST(ProcessBaseTest, logSystemInfo) {
  // Should not throw.
  EXPECT_NO_THROW(logSystemInfo());
}
