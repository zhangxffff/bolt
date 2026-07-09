/*
 * Copyright (c) ByteDance Ltd. and/or its affiliates.
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
 */

#include "bolt/functions/sparksql/specialforms/PackUntouchedColumns.h"

#include <algorithm>
#include <string_view>
#include <vector>

#include "bolt/expression/SpecialForm.h"
#include "bolt/row/dense/DenseRow.h"
#include "bolt/vector/ComplexVector.h"
#include "bolt/vector/DecodedVector.h"
#include "bolt/vector/FlatVector.h"

using namespace bytedance::bolt::exec;

namespace bytedance::bolt::functions::sparksql {
namespace {

RowTypePtr requireRowType(const TypePtr& type, const char* functionName) {
  BOLT_USER_CHECK_NOT_NULL(type);
  BOLT_USER_CHECK(
      type->isRow(),
      "{} expects ROW type but got {}.",
      functionName,
      type->toString());
  return std::dynamic_pointer_cast<const RowType>(type);
}

void requireVarbinaryType(const TypePtr& type, const char* functionName) {
  BOLT_USER_CHECK_NOT_NULL(type);
  BOLT_USER_CHECK(
      type->kind() == TypeKind::VARBINARY,
      "{} expects VARBINARY type but got {}.",
      functionName,
      type->toString());
}

RowVectorPtr flattenRowInput(
    const VectorPtr& input,
    const RowTypePtr& rowType,
    const SelectivityVector& rows,
    EvalCtx& context) {
  auto loadedInput = BaseVector::loadedVectorShared(input);
  if (auto* rowVector = loadedInput->as<RowVector>()) {
    return std::static_pointer_cast<RowVector>(loadedInput);
  }

  auto flatInput = BaseVector::create(rowType, rows.end(), context.pool());
  flatInput->copy(loadedInput.get(), rows, nullptr, false);
  return std::static_pointer_cast<RowVector>(flatInput);
}

class PackedSerializerExpr : public SpecialForm {
 public:
  PackedSerializerExpr(
      TypePtr type,
      std::vector<ExprPtr>&& inputs,
      bool trackCpuUsage)
      : SpecialForm(
            std::move(type),
            std::move(inputs),
            PackedSerializerCallToSpecialForm::kPackedSerialize,
            false,
            trackCpuUsage) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override {
    VectorPtr input;
    inputs_[0]->eval(rows, context, input);

    auto resultType = std::const_pointer_cast<const Type>(type_);
    LocalSelectivityVector activeRowsHolder(context, rows);
    auto* activeRows = activeRowsHolder.get();
    context.deselectErrors(*activeRows);

    if (activeRows->hasSelections()) {
      const auto rowType =
          requireRowType(inputs_[0]->type(), name().c_str());
      auto rowVector = flattenRowInput(input, rowType, *activeRows, context);

      VectorPtr localResult =
          BaseVector::create<FlatVector<StringView>>(
              resultType, rows.end(), context.pool());
      auto* flatResult = localResult->asUnchecked<FlatVector<StringView>>();

      if (rowVector->mayHaveNulls()) {
        activeRows->applyToSelected([&](vector_size_t row) {
          BOLT_USER_CHECK(
              !rowVector->isNullAt(row),
              "{} cannot serialize a top-level null ROW.",
              name());
        });
      }

      row::DenseRow denseRow(rowVector);
      const auto numRows = denseRow.numRows();
      std::vector<size_t> offsets(numRows);
      size_t totalSize = 0;
      for (vector_size_t row = 0; row < numRows; ++row) {
        offsets[row] = totalSize;
        totalSize += denseRow.rowSizeAt(row);
      }

      char* buffer = nullptr;
      if (numRows > 0) {
        buffer = flatResult->getRawStringBufferWithSpace(
            std::max<size_t>(totalSize, 1), true);
        denseRow.serialize(
            reinterpret_cast<uint8_t*>(buffer),
            folly::Range<const size_t*>(offsets.data(), offsets.size()));
      }

      activeRows->applyToSelected([&](vector_size_t row) {
        const auto size = denseRow.rowSizeAt(row);
        flatResult->setNoCopy(row, StringView(buffer + offsets[row], size));
      });

      context.moveOrCopyResult(localResult, *activeRows, result);
      context.releaseVector(localResult);
    }

    if (context.errors()) {
      EvalCtx::addNulls(
          rows, activeRows->asRange().bits(), context, resultType, result);
    }
    context.releaseVector(input);
  }

  void computePropagatesNulls() override {
    propagatesNulls_ = false;
  }
};

class PackedDeserializerExpr : public SpecialForm {
 public:
  PackedDeserializerExpr(
      TypePtr type,
      std::vector<ExprPtr>&& inputs,
      bool trackCpuUsage)
      : SpecialForm(
            std::move(type),
            std::move(inputs),
            PackedDeserializerCallToSpecialForm::kPackedDeserialize,
            false,
            trackCpuUsage) {}

  void evalSpecialForm(
      const SelectivityVector& rows,
      EvalCtx& context,
      VectorPtr& result) override {
    VectorPtr input;
    inputs_[0]->eval(rows, context, input);

    auto resultType = std::const_pointer_cast<const Type>(type_);
    const auto rowType = requireRowType(resultType, name().c_str());
    LocalSelectivityVector activeRowsHolder(context, rows);
    auto* activeRows = activeRowsHolder.get();
    context.deselectErrors(*activeRows);

    if (activeRows->hasSelections()) {
      LocalDecodedVector decoded(context, *input, *activeRows);
      if (decoded->mayHaveNulls()) {
        activeRows->applyToSelected([&](vector_size_t row) {
          BOLT_USER_CHECK(
              !decoded->isNullAt(row),
              "{} cannot deserialize a null packed ROW.",
              name());
        });
      }

      VectorPtr localResult = BaseVector::create(
          resultType, rows.end(), context.pool());

      std::vector<std::string_view> packedRows;
      packedRows.reserve(activeRows->countSelected());
      std::vector<vector_size_t> toSourceRow(rows.end(), 0);
      vector_size_t sourceRow = 0;
      activeRows->applyToSelected([&](vector_size_t row) {
        const auto value = decoded->valueAt<StringView>(row);
        packedRows.emplace_back(value.data(), value.size());
        toSourceRow[row] = sourceRow++;
      });

      auto unpacked =
          row::DenseRow::deserialize(packedRows, rowType, context.pool());
      localResult->copy(unpacked.get(), *activeRows, toSourceRow.data(), false);

      context.moveOrCopyResult(localResult, *activeRows, result);
      context.releaseVector(localResult);
    }

    if (context.errors()) {
      EvalCtx::addNulls(
          rows, activeRows->asRange().bits(), context, resultType, result);
    }
    context.releaseVector(input);
  }

  void computePropagatesNulls() override {
    propagatesNulls_ = false;
  }
};

} // namespace

TypePtr PackedSerializerCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  BOLT_USER_CHECK_EQ(
      argTypes.size(),
      1,
      "{} expects one argument.",
      kPackedSerialize);
  requireRowType(argTypes[0], kPackedSerialize);
  return VARBINARY();
}

ExprPtr PackedSerializerCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  BOLT_USER_CHECK_EQ(
      compiledChildren.size(),
      1,
      "{} expects one argument.",
      kPackedSerialize);
  requireVarbinaryType(type, kPackedSerialize);
  requireRowType(compiledChildren[0]->type(), kPackedSerialize);
  return std::make_shared<PackedSerializerExpr>(
      type, std::move(compiledChildren), trackCpuUsage);
}

TypePtr PackedDeserializerCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& argTypes) {
  BOLT_USER_CHECK_EQ(
      argTypes.size(),
      1,
      "{} expects one argument.",
      kPackedDeserialize);
  requireVarbinaryType(argTypes[0], kPackedDeserialize);
  BOLT_FAIL(
      "{} requires an explicit ROW return type.",
      kPackedDeserialize);
}

ExprPtr PackedDeserializerCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<ExprPtr>&& compiledChildren,
    bool trackCpuUsage,
    const core::QueryConfig& /*config*/) {
  BOLT_USER_CHECK_EQ(
      compiledChildren.size(),
      1,
      "{} expects one argument.",
      kPackedDeserialize);
  requireRowType(type, kPackedDeserialize);
  requireVarbinaryType(compiledChildren[0]->type(), kPackedDeserialize);
  return std::make_shared<PackedDeserializerExpr>(
      type, std::move(compiledChildren), trackCpuUsage);
}

} // namespace bytedance::bolt::functions::sparksql
