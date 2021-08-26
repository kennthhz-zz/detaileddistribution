include make_config.mk

ifndef DISABLE_JEMALLOC
	ifdef JEMALLOC
		PLATFORM_CXXFLAGS += -DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE
	endif
	EXEC_LDFLAGS := $(JEMALLOC_LIB) $(EXEC_LDFLAGS) -lpthread
	PLATFORM_CXXFLAGS += $(JEMALLOC_INCLUDE)
endif

CFLAGS += -Wstrict-prototypes

.PHONY: clean librocksdb

all:async_demo

async_demo: async_demo.cc
	$(CXX) $(CXXFLAGS) $@.cc -g -o$@ ../../rocksdb/librocksdb.a -I../../rocksdb/include -I../include -I../../liburing/src/include -L../../liburing/src/ -std=c++20 -fcoroutines -luring -D_GUN_SOURCE -fpermissive $(PLATFORM_LDFLAGS) $(PLATFORM_CXXFLAGS) $(EXEC_LDFLAGS)
clean:
	rm -rf ./async_demo
