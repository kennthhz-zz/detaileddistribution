/* MIT License

Copyright (c) 2021 Hao Zhang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. */
#pragma once

#include <limits.h>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include "iexecutor.h"

namespace dd {
/**
 * @brief A strategy implementation that spread all partitions evenly among Vcores.
 *
 */
class DefaultPartitionPlacementStrategy final : public IPartitionPlacementStrategy {
 public:
  /**
   * @brief Construct a new Default Partition Placement Strategy object
   *
   * @param vcores vcores that are bound to the placement strategy
   */
  explicit DefaultPartitionPlacementStrategy(std::vector<int>&& vcores) : vcores_{std::move(vcores)} {
    partition_to_vcore_ = std::make_shared<std::unordered_map<std::string, int>>();
    for (auto &v : vcores_)
      vcore_to_partition_.emplace(v, std::make_shared<std::set<std::string>>());
  }

  void AddPartition(const std::string& partition_id) override {
    std::lock_guard<std::mutex> guard(lock_);

    if (partition_to_vcore_->find(partition_id) != partition_to_vcore_->end()) {
      return;
    }

    auto low_size = UINT_MAX;
    auto low_vcore = -1;
    auto tmp_partition_to_vcore =
        std::make_shared<std::unordered_map<std::string, int>>(*partition_to_vcore_);

    for (auto const& [key, val] : vcore_to_partition_) {
      if (val->size() < low_size) {
        low_size = val->size();
        low_vcore = key;
      }
    }

    auto tmp_partitions =
        std::make_shared<std::set<std::string>>(*(vcore_to_partition_.at(low_vcore)));
    tmp_partitions->insert(partition_id);
    tmp_partition_to_vcore->emplace(partition_id, low_vcore);

    std::atomic_store(&partition_to_vcore_, tmp_partition_to_vcore);
    std::atomic_store(&(vcore_to_partition_.at(low_vcore)), tmp_partitions);
  }

  // Throw an exception for a nonexistent partition_id
  int GetPartitionVcore(const std::string& partition_id) override {
    auto tmp_partition_to_vcore = std::atomic_load(&partition_to_vcore_);
    return tmp_partition_to_vcore->at(partition_id);
  }

  auto GetVcorePartitions(int vcore) { return std::atomic_load(&vcore_to_partition_.at(vcore)); }

  void RemovePartition(const std::string& partition_id) override {
    std::lock_guard<std::mutex> guard(lock_);

    if (partition_to_vcore_->find(partition_id) == partition_to_vcore_->end()) {
      return;
    }

    auto tmp_partition_to_vcore =
        std::make_shared<std::unordered_map<std::string, int>>(*partition_to_vcore_);

    auto vcore = tmp_partition_to_vcore->at(partition_id);
    auto tmp_partitions = std::make_shared<std::set<std::string>>(*(vcore_to_partition_.at(vcore)));

    tmp_partition_to_vcore->erase(partition_id);
    tmp_partitions->erase(partition_id);

    std::atomic_store(&partition_to_vcore_, tmp_partition_to_vcore);
    std::atomic_store(&(vcore_to_partition_.at(vcore)), tmp_partitions);
  }

  const std::vector<int>& get_vcores() const override { return vcores_; }

 private:
  std::vector<int> vcores_;
  std::shared_ptr<std::unordered_map<std::string, int>> partition_to_vcore_;
  std::unordered_map<int, std::shared_ptr<std::set<std::string>>> vcore_to_partition_;
  std::mutex lock_;  // for remove and add
};

}  // namespace dd