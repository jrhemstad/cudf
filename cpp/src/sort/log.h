
#pragma once

#include <cudf/types.hpp>
#include <cudf/detail/nvtx/ranges.hpp>
#include <memory>

std::unique_ptr<cudf::process_range> log_sort(cudf::table_view t);
