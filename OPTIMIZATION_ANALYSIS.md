# Bolt 代码优化分析报告

> 基于对 Bolt 全部模块的深度代码分析，按模块划分潜在优化方向并预估收益。

## 项目概览

Bolt 是字节跳动基于 Velox 开发的 C++ 数据处理加速库，约 **2,863 个源文件**，核心模块包括：

| 模块 | 代码量 (LOC) | 职责 |
|------|-------------|------|
| exec | ~18,000+ | 执行引擎（算子、调度、Spill） |
| expression | ~22,500 | 表达式编译与向量化计算 |
| functions | ~56 MB | 标量/聚合/窗口函数库 |
| jit | ~24,000 | LLVM JIT 编译 |
| vector/buffer | ~15,000 | 向量编码与缓冲区管理 |
| common/memory | ~84,000 | 内存池、缓存、配置、压缩 |
| dwio | ~100,000+ | Parquet/ORC IO |
| shuffle | ~6,000+ | Spark Shuffle 实现 |
| connectors | ~4,000+ | Hive/S3/HDFS 连接器 |
| core | ~12,300 | 查询计划与配置 |
| type | ~38,000 | 类型系统与转换 |
| substrait | ~6,000 | Substrait 计划互操作 |

---

## 一、执行引擎 (exec) — 优化方向

### 1.1 Task 互斥锁竞争 ⭐⭐⭐

**问题：** `Task.cpp` 中使用单个 `std::timed_mutex` 保护所有 Driver 状态转换（启动、暂停、恢复、取消），在 128+ 并发 Driver 场景下成为瓶颈。

**位置：** `bolt/exec/Task.cpp`（3,239 行），51 处 lock/unlock 模式

**优化方向：**
- 将非冲突状态（isTerminated, pause_requested）改为 `std::atomic`
- 仅对关键路径保留 mutex（Promise 管理、Driver 队列操作）
- 考虑分片锁（per-pipeline 或 per-driver-group）

**预估收益：** 高并发场景下 Task 调度吞吐提升 **15-30%**，尤其在多 Driver 竞争密集时。

---

### 1.2 JoinBridge 同步瓶颈 ⭐⭐⭐

**问题：** `JoinBridge` 使用单个 `std::mutex` 串行化所有 HashBuild 和 HashProbe Driver 的协调操作。

**位置：** `bolt/exec/JoinBridge.h:52`，`bolt/exec/HashJoinBridge.cpp`

**优化方向：**
- 使用 lock-free atomic flag 标记 hash table 就绪状态
- 仅在 spill 数据恢复时加锁
- Build 完成 → Probe 启动路径使用无锁通知机制

**预估收益：** 大规模 Join 查询（10+ builder/prober）下延迟降低 **10-20%**。

---

### 1.3 Operator Stats 锁竞争 ⭐⭐

**问题：** 每个 Operator 每次 `getOutput()/addInput()` 都通过 `folly::Synchronized<OperatorStats>` 的 `wlock()` 更新统计信息。

**位置：** `bolt/exec/Operator.h:645`，`bolt/exec/Driver.cpp:706,721`

**优化方向：**
- 使用 Thread-Local Storage (TLS) 累积统计，定期 flush
- 或改用无锁 counter（`std::atomic`）做热路径计数

**预估收益：** 每个 Operator 调用减少 ~50ns 锁开销，整体查询 CPU 开销降低 **3-5%**。

---

### 1.4 HashTable 线性探测与 Tombstone 累积 ⭐⭐

**问题：** `HashTable.cpp`（2,337 行）使用线性探测，Tombstone 会导致探测链变长和缓存失效。

**位置：** `bolt/exec/HashTable.cpp`

**优化方向：**
- 定期 Tombstone 清理（idle period compact）
- 考虑 Robin Hood Hashing 改善分布均匀性
- 自适应 rehash 阈值

**预估收益：** Hash Join/Aggregation 在高删除率场景下性能提升 **5-15%**。

---

### 1.5 RowContainer 非连续内存迭代 ⭐⭐

**问题：** 行存储跨多个 allocation 分散，排序/聚合时随机访问导致 L3 cache miss。

**位置：** `bolt/exec/RowContainer.cpp`（1,686 行）

**优化方向：**
- 热路径算子（OrderBy、Aggregation）使用列式存储路径
- 批量行访问，提高 prefetch 友好度
- 基于预估 group count 预分配

**预估收益：** 大数据量排序/聚合场景，cache miss 减少，性能提升 **5-10%**。

---

### 1.6 Spiller 合并排序开销 ⭐

**问题：** 溢出数据恢复时需要重新排序，TreeOfLosers 合并流开销较大。

**位置：** `bolt/exec/Spiller.cpp:369`

**优化方向：**
- Spill 文件头保存排序元信息，读取时验证有序性跳过 re-sort
- 批量合并操作

**预估收益：** Spill 恢复场景下 IO + CPU 开销降低 **10-20%**。

---

## 二、表达式引擎 (expression) — 优化方向

### 2.1 表达式融合 (Expression Fusion) ⭐⭐⭐

**问题：** 当前架构支持但未系统化地将多个简单操作合并为单个向量化 kernel。

**位置：** `bolt/expression/Expr.cpp`（2,105 行），`bolt/expression/ExprCompiler.cpp`（667 行）

**优化方向：**
- 识别连续的算术/比较操作链，生成融合 kernel
- 利用 JIT 模块（已有 LLVM 基础设施）自动融合
- 减少中间 Vector 分配

**预估收益：** 复杂表达式查询（5+ 连续操作）性能提升 **20-40%**，减少 50% 中间内存分配。

---

### 2.2 自适应求值路径选择 ⭐⭐

**问题：** 当前 `evalFlatNoNulls` 快速路径仅在输入全为 Flat/Constant 且无 Null 时启用。

**位置：** `bolt/expression/Expr.h:207-217`，`bolt/expression/VectorFunction.h:114`

**优化方向：**
- 基于运行时统计（batch 大小、null 密度、编码类型）动态选择求值路径
- 为高频模式（Dictionary 输入、稀疏 Null）添加专用快速路径

**预估收益：** 中等复杂度表达式提速 **10-15%**。

---

### 2.3 JIT 编译扩展 ⭐⭐

**问题：** JIT 已有 LLVM ORCv2 基础设施（`ThrustJIT.cpp` 14,286 行），但 `CompileOnDemandLayer` 被注释掉，JIT 覆盖面有限。

**位置：** `bolt/jit/ThrustJIT.cpp:61-65`，`bolt/jit/expression/ExprJitCompiler.cpp`（15,654 行）

**优化方向：**
- 启用 CompileOnDemand 延迟编译，减少启动开销
- 基于运行时输入模式动态特化（如固定长度字符串、特定数值范围）
- 扩展 JIT 覆盖到聚合函数的 accumulate 热路径

**预估收益：** 热点表达式（重复执行 1000+ 次）性能提升 **30-50%**。

---

### 2.4 Dictionary 编码优化 ⭐

**问题：** `DictionaryVector-inl.h` 中 TODO 标记："optimize to reuse index vector"（基数增长操作）。

**位置：** `bolt/vector/DictionaryVector-inl.h`

**优化方向：**
- Dictionary index 复用，避免基数增长时重建
- Dictionary-on-Dictionary 场景的展平优化

**预估收益：** 低基数列操作提速 **5-10%**。

---

## 三、内存管理 (memory/vector/buffer) — 优化方向

### 3.1 VectorPool 容量调优 ⭐⭐

**问题：** 当前 VectorPool 每种类型仅缓存 10 个向量，最大 64K 元素，可能不足以覆盖高频场景。

**位置：** `bolt/vector/VectorPool.h:44-84`

**优化方向：**
- 基于工作负载动态调整池大小
- 增加大 batch（>64K）的复用支持
- 按 pipeline 隔离 VectorPool 减少竞争

**预估收益：** 减少 Vector 分配/释放开销，内存分配 CPU 开销降低 **5-8%**。

---

### 3.2 String Buffer 清理 ⭐

**问题：** `FlatVector-inl.h` 中 TODO："check and remove string buffers not referenced"。

**位置：** `bolt/vector/FlatVector-inl.h`，`bolt/vector/FlatVector.h:63-67`

**优化方向：**
- 定期检查并回收无引用的 string buffer
- 限制每个 FlatVector 的 string buffer 数量上限

**预估收益：** 长运行字符串密集查询内存占用降低 **5-15%**。

---

### 3.3 内存仲裁效率 ⭐⭐

**问题：** Driver 在紧密循环中检查 memory arbitration suspension 状态。

**位置：** `bolt/exec/MemoryReclaimer.cpp`，`bolt/exec/Driver.cpp:626`

**优化方向：**
- 使用 TLS flag 降低轮询频率
- 仅在 Operator 边界检查，而非每次循环

**预估收益：** 减少 ~1-2% 无效 CPU 轮询开销。

---

## 四、IO 与存储 (dwio/connectors) — 优化方向

### 4.1 数据缓存整合 ⭐⭐⭐

**问题：** `BufferedInput.h` 中 TODO："figure out how we can use the data cache for loaded data"。

**位置：** `bolt/dwio/common/BufferedInput.h:112`

**优化方向：**
- 将已加载数据自动注册到 AsyncDataCache
- 实现 cache-aware 的 prefetch 策略
- SSD 缓存区域大小从固定 256KB 改为自适应

**预估收益：** 重复扫描场景 IO 减少 **30-50%**，冷启动后续查询加速显著。

---

### 4.2 BitPack 解码 SIMD 化 ⭐⭐

**问题：** `BitPackDecoder.h`（825 行）中部分解码路径未使用 SIMD。

**位置：** `bolt/dwio/common/BitPackDecoder.h:668`

**优化方向：**
- 使用 AVX2/NEON intrinsics 加速 bit-packing 操作
- 批量解码模式

**预估收益：** Parquet/ORC 整数列解码速度提升 **20-40%**。

---

### 4.3 Region 合并距离调优 ⭐

**问题：** IO region 合并距离固定为 1.25MB，可能不适合所有场景。

**位置：** `bolt/dwio/common/BufferedInput.cpp:42-92`

**优化方向：**
- 基于存储类型（SSD vs HDD vs S3）动态调整合并距离
- 小文件场景降低阈值减少无效读取

**预估收益：** 不同存储介质上 IO 效率提升 **5-15%**。

---

## 五、Shuffle — 优化方向

### 5.1 自适应并行压缩阈值 ⭐⭐

**问题：** 并行压缩阈值硬编码为 2MB，代码中 TODO 标记 "make configurable"。

**位置：** `bolt/shuffle/sparksql/CompressionStream.h:52`

**优化方向：**
- 基于 CPU 核数和内存压力动态调整阈值
- 支持运行时切换压缩算法（LZ4 快速 vs ZSTD 高压缩比）

**预估收益：** Shuffle 密集查询压缩/解压吞吐提升 **10-20%**。

---

### 5.2 Shuffle Batch 大小估算 ⭐

**问题：** 当前基于固定 10KB/partition 估算 batch 大小。

**位置：** `bolt/shuffle/sparksql/BoltShuffleWriter.cpp:320-321`

**优化方向：**
- 基于实际数据特征自适应估算
- 运行时收集统计反馈调整

**预估收益：** 减少内存浪费和不必要的 spill，Shuffle 效率提升 **5-10%**。

---

## 六、查询规划 (core) — 优化方向

### 6.1 聚合 Spilling 完整性 ⭐⭐

**问题：** TODO："Add spilling for aggregations over distinct inputs"。

**位置：** `bolt/core/PlanNode.cpp:264,271`

**优化方向：**
- 实现 distinct 输入聚合的 spill 支持
- 完善 pre-grouped aggregation spilling

**预估收益：** 避免大数据量 distinct 聚合 OOM，**稳定性提升**，间接提升 10-20% 可用内存。

---

### 6.2 自适应 Filter 重排序 ⭐

**问题：** `adaptive_filter_reordering_enabled` 配置存在但效果有限。

**位置：** `bolt/core/QueryConfig.h`

**优化方向：**
- 基于运行时选择率统计动态重排 AND/OR 条件
- 高选择性 filter 前置，低选择性后置

**预估收益：** 多条件过滤查询提速 **5-15%**。

---

## 七、缓存系统 (common/caching) — 优化方向

### 7.1 缓存淘汰策略优化 ⭐⭐

**问题：** 当前评分公式 `(now - lastUse) / (1 + numUses)` 较简单，未考虑数据大小。

**位置：** `bolt/common/caching/AsyncDataCache.h:91-96`

**优化方向：**
- 引入数据大小权重（大缓存条目淘汰收益更高）
- 考虑时间局部性（短期高频 vs 长期低频）
- 启用 TTL 策略（CacheTTLController 已实现但未生产化）

**预估收益：** 缓存命中率提升 **5-10%**，有效内存利用率提升。

---

## 综合优化收益汇总

| 优先级 | 优化方向 | 模块 | 预估收益 | 实现难度 |
|--------|---------|------|---------|---------|
| P0 | 表达式融合 + JIT 扩展 | expression/jit | 20-50% (表达式密集查询) | 高 |
| P0 | Task/JoinBridge 锁优化 | exec | 15-30% (高并发场景) | 中 |
| P0 | 数据缓存整合 | dwio/common | 30-50% (重复扫描) | 中 |
| P1 | BitPack SIMD 解码 | dwio | 20-40% (整数列解码) | 中 |
| P1 | HashTable Tombstone 治理 | exec | 5-15% (高删除率) | 低 |
| P1 | Operator Stats 无锁化 | exec | 3-5% (全局) | 低 |
| P1 | 自适应并行压缩 | shuffle | 10-20% (Shuffle 密集) | 低 |
| P1 | 聚合 Distinct Spilling | core | 稳定性提升 | 中 |
| P2 | RowContainer 列式路径 | exec | 5-10% (排序/聚合) | 高 |
| P2 | VectorPool 动态调优 | vector | 5-8% (分配开销) | 低 |
| P2 | 自适应求值路径 | expression | 10-15% (中等表达式) | 中 |
| P2 | 缓存淘汰策略 | common | 5-10% (命中率) | 低 |
| P2 | Spiller 排序元信息保存 | exec | 10-20% (Spill 恢复) | 中 |
| P3 | String Buffer 清理 | vector | 5-15% (内存) | 低 |
| P3 | IO Region 合并调优 | dwio | 5-15% (IO 效率) | 低 |
| P3 | Filter 重排序 | core | 5-15% (多条件过滤) | 低 |
| P3 | Shuffle Batch 估算 | shuffle | 5-10% (Shuffle) | 低 |

**整体预估：** 如果 P0+P1 全部落地，端到端查询性能可提升 **20-40%**（取决于查询类型），内存效率提升 **10-20%**。
