/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

#include <cudf/column/column_device_view.cuh>
#include <cudf/table/table_device_view.cuh>
#include <cudf/table/table_view.hpp>
#include <utilities/error_utils.hpp>

#include <algorithm>

namespace cudf {

auto table_device_view::create(table_view source_view, cudaStream_t stream) {
  size_type total_descendants =
      std::accumulate(source_view.begin(), source_view.end(), 0,
                      [](size_type init, column_view col) {
                        return init + count_descendants(col);
                      });
  CUDF_EXPECTS(0 == total_descendants,
               "Columns with descendants are not yet supported.");
}

}  // namespace cudf
