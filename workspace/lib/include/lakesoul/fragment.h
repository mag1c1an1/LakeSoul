// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "lakesoul/data_reader.h"

#include <arrow/dataset/dataset.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace lakesoul {
class LakeSoulFragment : public arrow::dataset::Fragment {
public:
  explicit LakeSoulFragment(std::shared_ptr<arrow::Schema> schema);

  arrow::Result<arrow::dataset::RecordBatchGenerator> ScanBatchesAsync(
      const std::shared_ptr<arrow::dataset::ScanOptions> &options) override;

  std::string type_name() const override;

  arrow::Result<std::shared_ptr<arrow::Schema>>
  ReadPhysicalSchemaImpl() override;

  void AddFileUrl(const std::string &file_url);
  void AddFileUrls(const std::vector<std::string> &file_urls);

  void AddPrimaryKeys(const std::vector<std::string> &pks);

  void AddPartitionKeyValue(const std::string &key, const std::string &value);
  void AddPartitionKeyValues(
      const std::vector<std::pair<std::string, std::string>> &key_values);

  int GetBatchSize() const;
  void SetBatchSize(int batch_size);

  int GetThreadNum() const;
  void SetThreadNum(int thread_num);

  void CreateDataReader();

  void SetRetainPartitionColumns();

  void SetObjectStoreConfigs(
      const std::vector<std::pair<std::string, std::string>> &configs);

private:
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<std::string> file_urls_;
  std::vector<std::string> primary_keys_;
  std::vector<std::pair<std::string, std::string>> partition_info_;
  std::shared_ptr<LakeSoulDataReader> data_reader_;
  std::vector<std::pair<std::string, std::string>> object_store_configs_;
  int batch_size_ = 16;
  int thread_num_ = 1;
  bool retain_partition_columns_ = false;
};
} // namespace lakesoul