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
#pragma once

#include <cudf/column/column_device_view.cuh>
#include <cudf/types.hpp>

#include <cassert>

namespace cudf {

namespace detail {

template <typename ColumnDeviceView>
class table_device_view_base {
 public:
  table_device_view_base() = delete;
  ~table_device_view_base() = default;
  table_device_view_base(table_device_view_base const&) = default;
  table_device_view_base(table_device_view_base&&) = default;
  table_device_view_base& operator=(table_device_view_base const&) = default;
  table_device_view_base& operator=(table_device_view_base&&) = default;

  ColumnDeviceView* begin() noexcept { return _columns; }

  ColumnDeviceView* end() noexcept { return _columns + _num_columns; }

  ColumnDeviceView& column(size_type column_index) noexcept {
    assert(column_index > 0);
    assert(column_index < _num_columns);
    return _columns[column_index];
  }

  __host__ __device__ size_type num_columns() const noexcept {
    return _num_columns;
  }

  __host__ __device__ size_type num_rows() const noexcept { return _num_rows; }

 private:
  ColumnDeviceView* _columns;  ///< Array of view objects in device memory
  size_type _num_rows{};
  size_type _num_columns{};
};
}  // namespace detail

class table_device_view
    : public detail::table_device_view_base<column_device_view> {
  static auto create(table_view source_view, cudaStream_t stream = 0);
};

class mutable_table_device_view
    : public detail::table_device_view_base<mutable_column_device_view> {
  static auto create(mutable_table_view source_view, cudaStream_t stream = 0);
};
}  // namespace cudf