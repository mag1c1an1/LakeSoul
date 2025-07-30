/**
 * SPDX-FileCopyrightText: LakeSoul Contributors
 *
 * SPDX-License-Identifier: Apache-2.0
 */

#include <arrow/api.h>
#include <iostream>

arrow::Status RunMain() {
  // Builders are the main way to create Arrays in Arrow from existing values
  // that are not on-disk. In this case, we'll make a simple array, and feed
  // that in. Data types are important as ever, and there is a Builder for each
  // compatible type; in this case, int8.
  arrow::Int8Builder int8builder;
  int8_t days_raw[5] = {1, 12, 17, 23, 28};
  // AppendValues, as called, puts 5 values from days_raw into our Builder
  // object.
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
  return arrow::Status::OK();
}

int main() {
  arrow::Status st = RunMain();

  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }

  std::cout << "successed\n";
  return 0;
}