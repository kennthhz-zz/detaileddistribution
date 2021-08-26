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

#include <string>

namespace dd {
/**
 * @brief Partition placement strategy interface
 *
 */
class IPartitionPlacementStrategy {
 public:
  virtual ~IPartitionPlacementStrategy() {}

  /**
   * @brief Add a list of partitions to placement
   *
   * @param partitionIds a list of partitions by its Id
   */
  virtual void AddPartition(const std::string& partition_id) = 0;

  /**
   * @brief Remove a partition from placement
   *
   * @param partition_id partition Id to remove
   */
  virtual void RemovePartition(const std::string& partition_id) = 0;

  /**
   * @brief Get Vcore (index) of a particular partition
   *
   * @param partition_id partition to query
   * @return int Vcore index that his partition is place on
   */
  virtual int GetPartitionVcore(const std::string& partition_id) = 0;

  /**
   * @brief Get the vcores object
   * 
   * @return const std::vector<int>& 
   */
  virtual const std::vector<int>& get_vcores() const = 0;
};

} // namespace dd