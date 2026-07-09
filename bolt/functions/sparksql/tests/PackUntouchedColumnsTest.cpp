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

#include <gtest/gtest.h>

#include "bolt/common/base/tests/GTestUtils.h"
#include "bolt/functions/sparksql/specialforms/PackUntouchedColumns.h"
#include "bolt/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace bytedance::bolt::test;

namespace bytedance::bolt::functions::sparksql::test {
namespace {

class PackUntouchedColumnsTest : public SparkFunctionBaseTest {
 protected:
  VectorPtr packRoundTrip(const VectorPtr& payload) {
    const auto payloadType = payload->type();
    core::TypedExprPtr data =
        std::make_shared<const core::FieldAccessTypedExpr>(payloadType, "c0");
    auto serializeExpr = std::make_shared<const core::CallTypedExpr>(
        VARBINARY(),
        std::vector<core::TypedExprPtr>{data},
        PackedSerializerCallToSpecialForm::kPackedSerialize);
    auto packed =
        evaluate(serializeExpr, makeRowVector({"c0"}, {payload}));

    core::TypedExprPtr bytes =
        std::make_shared<const core::FieldAccessTypedExpr>(VARBINARY(), "c0");
    auto deserializeExpr = std::make_shared<const core::CallTypedExpr>(
        payloadType,
        std::vector<core::TypedExprPtr>{bytes},
        PackedDeserializerCallToSpecialForm::kPackedDeserialize);
    return evaluate(deserializeExpr, makeRowVector({"c0"}, {packed}));
  }

  RowVectorPtr makePayload() {
    auto nested = makeRowVector(
        {"x", "y"},
        {makeNullableFlatVector<int64_t>(
             {10, std::nullopt, 30, 40, 50}),
         makeNullableFlatVector<StringView>(
             {"ten", "twenty", std::nullopt, "", "fifty"})},
        [](vector_size_t row) { return row == 2; });

    return makeRowVector(
        {"i", "s", "arr", "map", "nested"},
        {makeNullableFlatVector<int32_t>(
             {1, std::nullopt, 3, 4, 5}),
         makeNullableFlatVector<StringView>(
             {"one", "", std::nullopt, "four", "five"}),
         makeArrayVectorFromJson<int64_t>(
             {"[1, 2]", "null", "[]", "[null, 5]", "[7]"}),
         makeMapVectorFromJson<int32_t, StringView>(
             {"{1: 'a'}", "{}", "null", "{4: null}", "{5: 'e'}"}),
         nested});
  }
};

TEST_F(PackUntouchedColumnsTest, roundTripNestedRow) {
  auto payload = makePayload();
  assertEqualVectors(payload, packRoundTrip(payload));
}

TEST_F(PackUntouchedColumnsTest, dictionaryWrappedInput) {
  auto payload = makePayload();
  auto dict = wrapInDictionary(makeIndices({4, 0, 2, 1, 0}), 5, payload);
  assertEqualVectors(dict, packRoundTrip(dict));
}

TEST_F(PackUntouchedColumnsTest, topLevelNullRowIsRejected) {
  auto payload = makeRowVector(
      {"i"},
      {makeFlatVector<int32_t>({1, 2, 3})},
      [](vector_size_t row) { return row == 1; });

  BOLT_ASSERT_USER_THROW(
      packRoundTrip(payload),
      "bolt_pack_serialize cannot serialize a top-level null ROW.");
}

TEST_F(PackUntouchedColumnsTest, nullPackedRowIsRejected) {
  auto packed = makeNullableFlatVector<StringView>(
      {std::nullopt}, VARBINARY());
  auto rowType = ROW({"i"}, {INTEGER()});
  core::TypedExprPtr bytes =
      std::make_shared<const core::FieldAccessTypedExpr>(VARBINARY(), "c0");
  auto deserializeExpr = std::make_shared<const core::CallTypedExpr>(
      rowType,
      std::vector<core::TypedExprPtr>{bytes},
      PackedDeserializerCallToSpecialForm::kPackedDeserialize);

  BOLT_ASSERT_USER_THROW(
      evaluate(deserializeExpr, makeRowVector({"c0"}, {packed})),
      "bolt_pack_deserialize cannot deserialize a null packed ROW.");
}

} // namespace
} // namespace bytedance::bolt::functions::sparksql::test
