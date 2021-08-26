/*
MIT License

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

#include <iostream>
#include <map>
#include <set>
#include <string>
#include <chrono>
#include <thread>

#include "iexecutor.h"
#include "IPartitionPlacementStrategy.h"
#include "MpScQueue.h"

namespace dd {

using namespace std::chrono_literals;
static constexpr auto idle_before_sleep = 500 * 1ms;
static constexpr auto sleep_length = 1000 * 1ms;

thread_local int current_thread_executor_vcore;

class Executor final : public IExecutor {
  Executor(const Executor&) = delete;
  Executor& operator=(const Executor&) = delete;
  Executor(Executor&&) = delete;
  Executor& operator=(Executor&&) = delete;

 public:
  explicit Executor(std::unique_ptr<IPartitionPlacementStrategy> placement_strategy) : 
    placement_strategy_{std::move(placement_strategy)} {

    for (auto& vcore : placement_strategy_->get_vcores()) {
      mpsc_executors_.emplace_back(std::make_unique<MpScExecutor>(vcore));
    }
  }

  void Start() override {
    for (auto& e : mpsc_executors_) {
      e->Start(this);
    }
  }

  void Shutdown() override {
    for (auto& e : mpsc_executors_)
        e->Shutdown();
    std::cout << "shutdown executor" << std::endl;
  }

  bool AddCPUTask(TaskFunc&& task_func,
                  const std::string& partition_id,
                  TaskPriority priority) override {
    try {
      auto idx = placement_strategy_->GetPartitionVcore(partition_id);
      mpsc_executors_[idx]->Add(std::forward<IExecutor::TaskFunc>(task_func), priority, false);
    } catch (std::exception& ex) {
      return false;
    }

    return true;
  }

  bool AddCPUTask(TaskFunc&& task_func, TaskPriority priority) override {
    try {
      auto idx = current_thread_executor_vcore;
      mpsc_executors_[idx]->Add(std::forward<IExecutor::TaskFunc>(task_func), priority, true);
    } catch (std::exception& ex) {
      return false;
    }

    return true;
  }

 private:
  class MpScExecutor final {
   public:
    explicit MpScExecutor(int vcore) : vcore_(vcore), counter_{0}, sleep_after_idle_{false} {
    }

    void Start(IExecutor *executor) {
      thread_ = std::thread(Execute, this, TaskArgs(executor), &idle_condition_mtx_, &idle_condition_, vcore_, &sleep_after_idle_);
      if (vcore_ != -1) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(vcore_, &cpuset);
        int rc = pthread_setaffinity_np(thread_.native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0)
          throw "can't bind to thread";
      }
    }

    void Shutdown() {
      Add([this](const TaskArgs){ this->shutdown_ = true; }, TaskPriority::kHigh, false);
      thread_.join();
    }

    void Add(IExecutor::TaskFunc&& task_func, TaskPriority priority, bool internal) {
      queues_[priority - 1].enqueue(std::forward<IExecutor::TaskFunc>(task_func));

      if (!internal) {
        std::unique_lock<std::mutex> lk(idle_condition_mtx_);
        if (sleep_after_idle_)
          idle_condition_.notify_one();
      }
    }

   private:
    void static Execute(MpScExecutor* e, 
      const TaskArgs& taskArg, 
      std::mutex* idle_condition_mtx, 
      std::condition_variable* idle_condition,
      int vcore,
      bool* sleep_after_idle) {
      
      current_thread_executor_vcore = vcore;
      auto last_dequeue = std::chrono::steady_clock::now();
      while (true) {
        if (e->shutdown_)
          break;

        auto tmp = (e->counter_++) % TaskPriority_TotalWeight() + 1;
        auto p = TaskPriority::kLowest;
        if (tmp >= 7)
          p = TaskPriority::kHigh;
        else if (tmp >= 4)
          p = TaskPriority::kMedium;
        else if (tmp >= 2)
          p = TaskPriority::kLow;

        IExecutor::TaskFunc task;
        bool found = false;
        if (!e->queues_[p - 1].dequeue(task)) {
          for (int8_t p1 = TaskPriority::kHigh; p1 >= TaskPriority::kLowest; p1--) {
            if (e->queues_[p1 - 1].dequeue(task)) {
              found = true;
              break;
            }
          }
        } else {
          found = true;
        }

        if (found) {
          task(taskArg);
          last_dequeue = std::chrono::steady_clock::now();
          continue;
        } else {
          auto current_time = std::chrono::steady_clock::now();
          auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(current_time - last_dequeue);
          if (diff > idle_before_sleep) {
            std::unique_lock<std::mutex> lk(*idle_condition_mtx);
            *sleep_after_idle = true;
            idle_condition->wait_for(lk, sleep_length);
          }
        }
      }
    }

   private:
    MpScQueue<IExecutor::TaskFunc> queues_[IExecutor::TaskPriority_Count];
    const int vcore_;
    std::thread thread_;
    uint_fast64_t counter_;
    bool shutdown_;
    std::mutex idle_condition_mtx_;
    std::condition_variable idle_condition_;
    bool sleep_after_idle_;
  };

 private:

  using MpScExecutorPtr = std::unique_ptr<MpScExecutor>;
  using MpScExecutorPtrs = std::vector<MpScExecutorPtr>;
  MpScExecutorPtrs mpsc_executors_;
  std::unique_ptr<IPartitionPlacementStrategy> placement_strategy_;
  std::atomic<bool> shutdown_;
};
}  // namespace dd