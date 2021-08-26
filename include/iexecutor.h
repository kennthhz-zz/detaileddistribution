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
SOFTWARE.*/
#pragma once

#include <string>

#include "IPartitionPlacementStrategy.h"
#include "folly/Function.h"

namespace dd {
/**
 * @brief 
 * Interface for executing task. 
 *
 */
class IExecutor {
 public:
  virtual ~IExecutor() = default;

  struct TaskArgs;
  using TaskFunc = folly::Function<void(const TaskArgs&)>;

  enum TaskPriority : std::int8_t {
    kLowest = 1,
    kLow = 2,
    kMedium = 3,
    kHigh = 4,
  };

  static const std::int8_t TaskPriority_Count = 4;
  static constexpr std::int8_t TaskPriority_TotalWeight() {
    auto total = 0;
    for (int8_t p = TaskPriority::kLowest; p <= TaskPriority::kHigh; p++)
      total += p;
    return total;
  }

  /**
   * @brief Start
   *
   */
  virtual void Start() = 0;

  /**
   * @brief After calling this method, consumer can no longer use TaskQueue.All dynamically
   * allocated resources such as task queue and threads will be reclaimed.
   *
   */
  virtual void Shutdown() = 0;

  /**
   * @brief Schedule a non-blockingcompute task.
   *
   * @param task_func task itself
   * @param partition_id task's partition Id. If partition id is "", it's a common task, does not
   * belong to any partition.
   * @param priority task's priority
   *
   * @return true
   * @return false
   *
   */
  virtual bool AddCPUTask(TaskFunc&& task_func,
                          const std::string& partition_id,
                          TaskPriority priority) = 0;

  /**
   * @brief Schedule a non-blockingcompute task in the same thread/vcore as the calling thread
   *
   * @param task_func task itself
   * @param priority task's priority
   *
   * @return true
   * @return false
   *
   */
  virtual bool AddCPUTask(TaskFunc&& task_func,
                          TaskPriority priority) = 0;
};

/**
 * @brief task's arguments
 *
 */
struct IExecutor::TaskArgs {
  // used for next task.
  IExecutor* executor;

  // maybe we can put some resource context in here.
  explicit TaskArgs(IExecutor* executor) : executor(executor) {}
};

}  // namespace dd