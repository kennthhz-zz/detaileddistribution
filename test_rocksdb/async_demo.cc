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

#include <time.h>
#include <string>
#include <memory>
#include <iostream>
#include <thread>         // std::this_thread::sleep_for
#include <chrono>         // std::chrono::seconds
#include <exception>
#include "liburing.h"

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "rocksdb/async_result.h"
#include "executor.h"
#include "placement_strategy.h"
#include <unordered_map>

using namespace ROCKSDB_NAMESPACE;
using namespace dd;

int total_reads = 100000;
std::atomic<int> completed_read_count = 0;
static uint64_t RingSize = 1000;
static bool direct_io_read = false;
static int completion_batch_size = 100;

constexpr int num_keys = 512 * 1024;
const std::string kDBPath = "/tmp";

struct DbInfo {
  DB* db;
  struct io_uring* io_uring;
};

std::unordered_map<std::string, DbInfo> DbInfos;
using namespace ROCKSDB_NAMESPACE;
using namespace std::chrono_literals;

void WriteDB(DB* db, int num_keys, int size) {
  Status s;
  WriteOptions write_options; 
  write_options.disableWAL = false;
  char value[size];
  memset(value, '#', size);

  for (int i = 0; i < num_keys; i++) {
    s = db->Put(write_options, std::to_string(i), value);
    assert(s.ok());
  }
}

static void msec_to_ts(struct __kernel_timespec *ts, unsigned int msec)
{
	ts->tv_sec = msec / 1000;
	ts->tv_nsec = (msec % 1000) * 1000000;
}

static void reap_iouring_completion(struct io_uring *ring, std::string partition_id, IExecutor* executor) {
  struct io_uring_cqe *cqes[completion_batch_size];  
  while(true) {
    // Here are not using wait to block the completion thread.
    // We are busy polling completions using batched peak
    auto ret = io_uring_peek_batch_cqe(ring, cqes, completion_batch_size);
    if (ret > 0) {
      for (int i = 0; i < ret; i++) {
        struct file_page *rdata =(struct file_page *)io_uring_cqe_get_data(cqes[i]);
        io_uring_cqe_seen(ring, cqes[i]);
        auto h = std::coroutine_handle<async_result::promise_type>::from_promise(*rdata->promise);

        // invoke C++20 coroutine resumption.
        h.resume();
      }
    } else {
      break;
    }
  }
}

DB* OpenDb(std::string dbName) {
  DB* db;
  Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 20; //1mb
  options.max_write_buffer_number = 1;
  options.min_write_buffer_number_to_merge = 2;
  options.use_direct_reads = direct_io_read;
  options.use_direct_io_for_flush_and_compaction = false;
  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Status s = DB::Open(options, kDBPath + "/" + dbName, &db);
  assert(s.ok());
  return db;
}

async_result ReadOne(DB* db, ReadOptions ro, int key) {
  PinnableSlice get_value;
  auto a_result = db->AsyncGet(ro, db->DefaultColumnFamily(), std::to_string(key), &get_value, nullptr);
  co_await a_result;
  if (a_result.result().ok()) {
    completed_read_count++;
  }
}

void print_usage() {
  std::cout<<"usage: async_demo w\n";
  std::cout<<"usage: async_demo r --total_reads [number of read requests] --direct_io --ring_size [size of io_uring] --completion_batch_size [size of completion batch]\n";
}

int main(int argc, char *argv[]) {
  if  (argc < 2) {
    print_usage();
    return 0;
  }

  if (*argv[1] == 'w') {
      for (auto i = 0; i < 2; i++) {
        std::unique_ptr<DB> db(OpenDb("db_" + std::to_string(i)));
        std::cout<<"Write date to DB first\n";
        WriteDB(db.get(), num_keys, 1024);
        db->Close();
      }
      return 1;
  }

  if (*argv[1] != 'r') {
    print_usage();
    return 0;
  }

  for (int i = 2; i < argc; i++) {
    std::string opt(argv[i]);
    if (opt.compare("--total_reads") == 0) {
      total_reads = std::stoi(argv[i+1]);
      i++;
    } else if (opt.compare("--direct_io")==0) {
      direct_io_read = true;

    } else if (opt.compare("--ring_size")==0) {
      RingSize = std::stoi(argv[i+1]);
      i++;
    } else if (opt.compare("--completion_batch_size")==0) {
      completion_batch_size = std::stoi(argv[i+1]);
      i++;
    }
  }

  for (auto i = 0; i < 1; i++) {
    std::string dbName = "db_" + std::to_string(i);
    struct DbInfo info;
    info.db = OpenDb(dbName); 
    io_uring* ioring = new io_uring();
    auto ret = io_uring_queue_init(RingSize, ioring, 0);
    if( ret < 0) {
      fprintf(stderr, "queue_init: %s\n", strerror(-ret));
      return -1;
    }

    info.io_uring = ioring;
    DbInfos[dbName] = info;
  }

  // Here we use our task based, one-thread-per-core executor to run async read tasks.
  // To maximize the performance, the execution thread is bound to a specific vcore 0.
  // The task queue is lock free with one producer and one consumer. The async completion
  // is also done in the task under the same task queue. 
  // So there is no thread context switch in async processing (except for app's main thread which is doing the submission);
  // none of the task is ever blocked by IO; The CPU at vcore 0 should run at 100%. 
  std::vector<int> vcores = { 0 };
  auto strategy = std::make_unique<DefaultPartitionPlacementStrategy>(std::move(vcores));
  for (const auto& i : DbInfos) 
    strategy->AddPartition(i.first);
  auto executor = std::make_unique<Executor>(std::move(strategy));
  executor->Start();

  std::srand(std::time(nullptr)); 
  int db_index = 0;
  std::string db_name = "db_" + std::to_string(db_index);
  auto db = DbInfos[db_name].db;

  ReadOptions ro;
  ro.read_tier == kPersistedTier;
  auto io_uring_opt = new IOUringOptions(DbInfos[db_name].io_uring);
  ro.io_uring_option = io_uring_opt;

  auto start = std::chrono::steady_clock::now();
  for(auto i = 0; i < total_reads; i++) {
    auto r = std::rand();
    int key_index = r % num_keys;
    bool ret = executor->AddCPUTask([&ro, db, db_name, key_index](const IExecutor::TaskArgs& args) { 
          ReadOne(DbInfos[db_name].db, ro, key_index);
          }, db_name, IExecutor::TaskPriority::kMedium);

    // We need to sprinkle IO completion with IO submition task or io_uring submission queue will over flow. 
    if (i % 1000 == 0)
      executor->AddCPUTask([i, db, db_name, key_index, ex =executor.get()](const IExecutor::TaskArgs& args) { 
        reap_iouring_completion(DbInfos[db_name].io_uring, db_name, ex);
      }, db_name, IExecutor::TaskPriority::kMedium);
  }

  // This part is to simiulate continous IO completion after all async read requests have been submitted.
  while(true) {
    if (completed_read_count >= total_reads)
      break;
    std::this_thread::sleep_for(100ms);
    executor->AddCPUTask([db_name, ex =executor.get()](const IExecutor::TaskArgs& args) { 
      reap_iouring_completion(DbInfos[db_name].io_uring, db_name, ex);
    }, db_name, IExecutor::TaskPriority::kMedium);
  }

  auto stop = std::chrono::steady_clock::now();
  auto latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();

  std::cout<<"Total completed 1k read:"<<completed_read_count<<" in "<<(double)latency_ns/1000000000<<" seconds with average request latency:"<<((double)latency_ns/1000)/total_reads<<" usecs\n";
  std::cout<<"rps:"<<total_reads/((double)latency_ns/1000000000)<<"/s\n";
  executor->Shutdown();
}
