# A demo for Rocksdb's high performance async IO using Linux io_uring and C++20's coroutine

## pre-req:
You need to clone and compile my rocksdb fork that supports Async IO. The location of the fork is at https://github.com/kennthhz/rocksdb

## How to run async_demo
make all

**First, run the below command to create and write to a db with 512MB of 1kb sized kvs.**

sudo ./async_demo w 

**Now you can test with following command on random read. Following example will randomly read 300k kvs using direct_io (i.e. without going through OS's page cache, also rocksdb's blockcache is disable)**

sudo ./async_demo r --total_reads 3000000 --direct_io
