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

#include "bolt/expression/RegisterSpecialForm.h"
#include "bolt/expression/RowConstructor.h"
#include "bolt/expression/SpecialFormRegistry.h"
#include "bolt/functions/sparksql/specialforms/AtLeastNNonNulls.h"
#include "bolt/functions/sparksql/specialforms/DecimalRound.h"
#include "bolt/functions/sparksql/specialforms/FromJson.h"
#include "bolt/functions/sparksql/specialforms/GetArrayStructFields.h"
#include "bolt/functions/sparksql/specialforms/GetStructField.h"
#include "bolt/functions/sparksql/specialforms/JsonSplit.h"
#include "bolt/functions/sparksql/specialforms/MakeDecimal.h"
#include "bolt/functions/sparksql/specialforms/PackUntouchedColumns.h"
#include "bolt/functions/sparksql/specialforms/SparkCastExpr.h"
namespace bytedance::bolt::functions {
void registerSparkSpecialFormFunctions() {
  BOLT_REGISTER_VECTOR_FUNCTION(
      udf_concat_row, exec::RowConstructorCallToSpecialForm::kRowConstructor);
}

namespace sparksql {
void registerSpecialFormGeneralFunctions(const std::string& prefix) {
  registerSparkSpecialFormFunctions();
  exec::registerFunctionCallToSpecialForms();
  exec::registerFunctionCallToSpecialForm(
      MakeDecimalCallToSpecialForm::kMakeDecimal,
      std::make_unique<MakeDecimalCallToSpecialForm>());
  exec::registerFunctionCallToSpecialForm(
      DecimalRoundCallToSpecialForm::kRoundDecimal,
      std::make_unique<DecimalRoundCallToSpecialForm>());
  exec::registerFunctionCallToSpecialForm(
      AtLeastNNonNullsCallToSpecialForm::kAtLeastNNonNulls,
      std::make_unique<AtLeastNNonNullsCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "cast", std::make_unique<SparkCastCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      "try_cast", std::make_unique<SparkTryCastCallToSpecialForm>());

  registerFunctionCallToSpecialForm(
      "get_struct_field", std::make_unique<GetStructFieldToSpecialForm>());
  registerFunctionCallToSpecialForm(
      GetArrayStructFieldsCallToSpecialForm::kGetArrayStructFields,
      std::make_unique<GetArrayStructFieldsCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      FromJsonCallToSpecialForm::kFromJson,
      std::make_unique<FromJsonCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      PackedSerializerCallToSpecialForm::kPackedSerialize,
      std::make_unique<PackedSerializerCallToSpecialForm>());
  registerFunctionCallToSpecialForm(
      PackedDeserializerCallToSpecialForm::kPackedDeserialize,
      std::make_unique<PackedDeserializerCallToSpecialForm>());
}
} // namespace sparksql
} // namespace bytedance::bolt::functions
