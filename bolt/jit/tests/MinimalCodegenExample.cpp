/*
 * Minimal JIT codegen example: compile and run func(a, b) { return a + b; }
 *
 * Demonstrates two approaches:
 *   1. From LLVM IR string
 *   2. From LLVM IRBuilder API (programmatic codegen)
 */

#ifdef ENABLE_BOLT_JIT

#include <gtest/gtest.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Verifier.h>

#include "bolt/jit/ThrustJIT.h"

namespace bytedance::bolt::jit::test {

using namespace bolt::jit;

class MinimalCodegenTest : public ::testing::Test {
 protected:
  ThrustJIT* jit = ThrustJIT::getInstance();
};

// ============================================================
// Approach 1: From LLVM IR string
// The simplest way — write IR text directly.
// ============================================================
TEST_F(MinimalCodegenTest, AddFromIRString) {
  std::string funcName = "add_ir_string";

  // Step 1: Write LLVM IR that defines: int64 add(int64 a, int64 b) { return a + b; }
  const char* ir = R"IR(
    define i64 @add_ir_string(i64 %a, i64 %b) {
      %result = add i64 %a, %b
      ret i64 %result
    }
  )IR";

  // Step 2: Create a thread-safe module (each module gets its own LLVMContext)
  auto tsm = jit->CreateTSModule(funcName);

  // Step 3: Parse the IR string into the module
  tsm.withModuleDo([&](llvm::Module& m) {
    bool err = jit->AddIRIntoModule(ir, &m);
    ASSERT_FALSE(err) << "Failed to parse IR string";
  });

  // Step 4: Compile to native machine code
  CompiledModuleSP mod = jit->CompileModule(std::move(tsm));
  ASSERT_NE(mod, nullptr);

  // Step 5: Get function pointer and call it
  using AddFunc = int64_t (*)(int64_t, int64_t);
  auto func = (AddFunc)mod->getFuncPtr(funcName);
  ASSERT_NE(func, nullptr);

  EXPECT_EQ(func(1, 2), 3);
  EXPECT_EQ(func(100, 200), 300);
  EXPECT_EQ(func(-10, 10), 0);
}

// ============================================================
// Approach 2: From LLVM IRBuilder API
// Programmatic codegen — useful when IR depends on runtime info
// (e.g., column types, number of keys, etc.)
// ============================================================
TEST_F(MinimalCodegenTest, AddFromIRBuilder) {
  std::string funcName = "add_ir_builder";

  // Step 1: Create a thread-safe module
  auto tsm = jit->CreateTSModule(funcName);

  // Step 2: Build IR programmatically using IRBuilder
  tsm.withModuleDo([&](llvm::Module& module) {
    auto& ctx = module.getContext();
    llvm::IRBuilder<> builder(ctx);

    // 2a. Define function signature: int64_t add(int64_t, int64_t)
    auto* i64Ty = llvm::Type::getInt64Ty(ctx);
    auto* funcType = llvm::FunctionType::get(
        /*Result=*/i64Ty,
        /*Params=*/{i64Ty, i64Ty},
        /*isVarArg=*/false);
    auto* func = llvm::Function::Create(
        funcType, llvm::Function::ExternalLinkage, funcName, module);

    // 2b. Name the arguments for readability
    auto args = func->arg_begin();
    llvm::Value* a = args++;
    a->setName("a");
    llvm::Value* b = args++;
    b->setName("b");

    // 2c. Create entry basic block and emit: return a + b
    auto* entryBB = llvm::BasicBlock::Create(ctx, "entry", func);
    builder.SetInsertPoint(entryBB);
    llvm::Value* sum = builder.CreateAdd(a, b, "sum");
    builder.CreateRet(sum);

    // 2d. Verify the generated function
    ASSERT_FALSE(llvm::verifyFunction(*func, &llvm::errs()));
  });

  // Step 3: Compile to native machine code
  CompiledModuleSP mod = jit->CompileModule(std::move(tsm));
  ASSERT_NE(mod, nullptr);

  // Step 4: Get function pointer and call it
  using AddFunc = int64_t (*)(int64_t, int64_t);
  auto func = (AddFunc)mod->getFuncPtr(funcName);
  ASSERT_NE(func, nullptr);

  EXPECT_EQ(func(1, 2), 3);
  EXPECT_EQ(func(100, 200), 300);
  EXPECT_EQ(func(-10, 10), 0);
}

} // namespace bytedance::bolt::jit::test

#endif // ENABLE_BOLT_JIT
