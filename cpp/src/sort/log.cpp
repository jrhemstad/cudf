#include "log.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>

#include <cudf/table/table_view.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <string>

namespace {
inline std::string get_type_name(cudf::data_type type) {
  return cudf::experimental::type_dispatcher(
      type, cudf::experimental::type_to_name{});
}
}  // namespace

std::unique_ptr<cudf::process_range> log_sort(cudf::table_view t) {
  static std::shared_ptr<spdlog::logger> logger =
      std::make_shared<spdlog::logger>(
          "SORT", std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                      "sorts.txt", true));

  if (t.num_columns() == 1 and not t.column(0).has_nulls()) {
    std::string msg{"single column sort without nulls. Size: "};
    msg += std::to_string(t.num_rows());
    msg += " type: ";
    msg += get_type_name(t.column(0).type());
    logger->info(msg);
    return std::make_unique<cudf::process_range>("single_column_sort", nvtx3::rgb(255,105,180));
  }

  return nullptr;
}