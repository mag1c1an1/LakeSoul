// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <lakesoul_c_bindings.h>

#include <arrow/api.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace lakesoul {

class LakeSoulDataReader
    : public std::enable_shared_from_this<LakeSoulDataReader> {
public:
  LakeSoulDataReader(
      std::shared_ptr<arrow::Schema> schema,
      const std::vector<std::string> &file_urls,
      const std::vector<std::string> &primary_keys,
      const std::vector<std::pair<std::string, std::string>> &partition_info);

  int GetBatchSize() const;
  void SetBatchSize(int batch_size);

  int GetThreadNum() const;
  void SetThreadNum(int thread_num);

  void StartReader();
  bool IsFinished() const;
  arrow::Future<std::shared_ptr<arrow::RecordBatch>> ReadRecordBatchAsync();

  void SetRetainPartitionColumns();
  void SetObjectStoreConfigs(
      const std::vector<std::pair<std::string, std::string>> &configs);

private:
  IOConfig *CreateIOConfig();
  TokioRuntime *CreateTokioRuntime();
  std::shared_ptr<CResult<Reader>> CreateReader();

  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::string> file_urls_;
  std::vector<std::string> primary_keys_;
  std::vector<std::pair<std::string, std::string>> partition_info_;
  std::vector<std::pair<std::string, std::string>> object_store_configs_;
  int batch_size_ = 16;
  int thread_num_ = 1;
  std::shared_ptr<CResult<Reader>> reader_;
  bool finished_ = false;
  bool retain_partition_columns_ = false;
};
} // namespace lakesoul