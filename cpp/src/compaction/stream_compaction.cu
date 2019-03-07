/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Felipe Aramburu <felipe@blazingdb.com>
 *     Copyright 2018 Alexander Ocsa <alexander@blazingdb.com>
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

#include "cudf.h"
#include "rmm/thrust_rmm_allocator.h"
#include "stream_compaction.hpp"
#include "utilities/cudf_utils.h"
#include "utilities/error_utils.hpp"

namespace cudf {

/**
 * @brief Filters a column using a column of boolean values as a mask.
 *
 */
gdf_column gdf_apply_boolean_mask(gdf_column const *input,
                                  gdf_column const *boolean_mask) {
  CUDF_EXPECTS(input->size == boolean_mask->size, "Column size mistmatch");

  gdf_column output{};

  bool const mask_has_nulls{boolean_mask->valid != nullptr &&
                            boolean_mask->null_count > 0};
  bool const input_has_nulls{input->valid != nullptr && input->null_count > 0};

  if (mask_has_nulls && input_has_nulls) {
  } else if (mask_has_nulls) {
  } else if (input_has_nulls) {
  } else {
  }

  return output;
}

}  // namespace cudf