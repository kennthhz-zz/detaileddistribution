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

using namespace dd;

struct io_uring *ioring = nullptr;

const int BILLION = 1000000000L;
using namespace ROCKSDB_NAMESPACE;
using namespace std::chrono_literals;

std::string kDBPath = "/tmp/rocksdb_simple_example";

constexpr int total_count = 500000;
const int batch_size = 1024;
constexpr int batch_count = total_count / batch_size;

/**
 * @brief Generate 1kb sized value string
 * 
 * @return std::string 
 */
std::string generate_value_string() { 
  std::string value;
  for (int i = 0; i < 32; i++)
    value.append("123456789012345678901234567890ab");
  return value;
}

void generate_keys(std::vector<rocksdb::Slice>& results) {
  
  for (int i = 0; i < total_count; i++) {
    std::string s("key_");
    s.append(std::to_string(i));
    auto slice = Slice(s);
    results.push_back(slice);
  }
}

void WriteDB(DB* db, std::vector<rocksdb::Slice>& keys, std::string& value) {
  Status s;
  WriteOptions write_options; 
  write_options.disableWAL = false;
  for (int i = 0; i < total_count; i++) {
    s = db->Put(write_options, keys.at(i), value);
    //std::cout<<"Write key:"<<keys.at(i).data()<<" value:"<<value<<"\n";
    assert(s.ok());
  }
}

void SetCpu(std::thread& thread) {
  // affinitize thread to vcore 
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
  }
}

static void reap_iouring_completion(struct io_uring *ring, std::string partition_id, Executor* executor) {
  struct io_uring_cqe *cqe;
  while (true) {
    auto ret = io_uring_wait_cqe(ring, &cqe);
    if (ret == 0 && cqe->res >=0) {
      struct file_read_page *rdata =(file_read_page *)io_uring_cqe_get_data(cqe);
      io_uring_cqe_seen(ring, cqe);

      auto h = std::coroutine_handle<async_result::promise_type>::from_promise(*rdata->promise);
      executor->AddCPUTask([&h](const IExecutor::TaskArgs& args) { 
        h.resume();
        }, partition_id, IExecutor::TaskPriority::kHigh);
      return;
    }
  }
}

async_result work(bool write, bool async) {
  DB* db;
  Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 20; //1mb
  options.max_write_buffer_number = 1;
  options.min_write_buffer_number_to_merge = 2;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = false;
  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());
  std::unique_ptr<DB> u_db(db);

  // Generate keys and value
  std::vector<rocksdb::Slice> keys;
  for (int i = 0; i < total_count; i++) {
    auto s = new std::string("key_");
    s->append(std::to_string(i));
    auto slice = Slice(*s);
    keys.push_back(slice);
  }

  auto value = generate_value_string();

  if (write) {
    std::cout<<"Write date to DB first\n";
    WriteDB(db, keys, value);
  }

  std::cout<<"Hit any key to compact and read\n";
  std::string input;
  std::getline(std::cin, input);

  struct timespec start, end;
  PinnableSlice* pin_values = new PinnableSlice[batch_size];
  std::unique_ptr<PinnableSlice[]> pin_values_guard(pin_values);
  std::vector<Status> stat_list(batch_size);

  std::cout<<"start read\n";


  PinnableSlice get_value;
  ReadOptions ro;
  ro.read_tier == kPersistedTier;

  if (!async) {
    clock_gettime(CLOCK_MONOTONIC, &start);
    s = db->Get(ReadOptions(), db->DefaultColumnFamily(), keys[0], &get_value);
    clock_gettime(CLOCK_MONOTONIC, &end);
    std::cout<<"Get key: "<<keys[0].data()<<" return is "<<s.ok()<<"\n";
    std::cout<<"Result:"<<get_value.data()<<"\n";
  } else  {
    clock_gettime(CLOCK_MONOTONIC, &start);
    auto a_result = db->AsyncGet(ReadOptions(), db->DefaultColumnFamily(), keys[0], &get_value, nullptr);
    co_await a_result;
    clock_gettime(CLOCK_MONOTONIC, &end);
    std::cout<<"Async Get key: "<<keys[0].data()<<" return is "<<a_result.result().ok()<<"\n";
    std::cout<<"Result:"<<get_value.data()<<"\n";
  }

  uint64_t diff;
  diff = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  std::cout<<"Sequential read elapsed time: "<< diff <<" usec.\n";

  u_db->Close();
}

int main(int argc, char *argv[]) {

  std::vector<int> vcores = { 0, 1, 2, 3, 4 };
  std::string partition_id("1");
  auto strategy = std::make_unique<DefaultPartitionPlacementStrategy>(std::move(vcores));
  strategy->AddPartition(partition_id);
  auto executor = std::make_unique<Executor>(std::move(strategy));
  executor->Start();

  static const uint64_t RingSize = 1000;
  bool write = false;
  if (*argv[1] == 'w') {
    write = true;
  }

  bool async = false;
  if (*argv[2] == 'a') {
    async = true;
  }

  ioring = new io_uring();
  auto ret = io_uring_queue_init(RingSize, ioring, 0);
  if( ret < 0) {
    fprintf(stderr, "queue_init: %s\n", strerror(-ret));
    return -1;
  }
  
  std::thread t1(reap_iouring_completion, ioring, partition_id, executor.get());
  executor->AddCPUTask([write, async](const IExecutor::TaskArgs& args) { 
        work(write, async);
        }, partition_id, IExecutor::TaskPriority::kHigh);
  t1.join();
  executor->Shutdown();
}
