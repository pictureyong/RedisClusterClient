
obj-m += hiredis/

SUBDIRS=./hiredis
OBJ=Util.o RedisClusterClient.o RedisClusterClient_handle.o
LIBNAME=libredis_cluster_client
REDIS_CLIENT_SONAME=$(shell grep REDIS_CLIENT_SONAME RedisClusterClient.h | awk '{print $$3}')
TEST=test_redis_client
LDFLAGS=-L./hiredis

DYLIBSUFFIX=so
STLIBSUFFIX=a
DYLIB_MINOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(REDIS_CLIENT_SONAME)
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)

REAL_LDFLAGS=$(LDFLAGS)

# CC:=$(shell sh -c 'type $${CC%% *} >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
CC=g++
AR=ar
OPTIMIZATION?=-O3
WARNINGS=-Wall -W -Wstrict-prototypes -Wwrite-strings -Wno-missing-field-initializers
DEBUG_FLAGS?= -g -ggdb
REAL_CFLAGS=$(OPTIMIZATION) -fPIC $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(DEBUG_FLAGS)

DYLIB_MAKE_CMD=$(CC) -shared -Wl,-soname,$(DYLIB_MINOR_NAME)
DYLIBNAME=$(LIBNAME).$(DYLIBSUFFIX)
STLIB_MAKE_CMD=$(AR) crv

all: $(OBJ) $(STLIBNAME) $(TEST)
#	make -C hiredis all
#	g++ -c -std=c++11 RedisClusterClient.cpp RedisClusterClient_handle.cpp Util.cpp
#	ar crv libredisclient.a RedisClusterClient.o RedisClusterClient_handle.o Util.o
#	g++ -o test_cluster -std=c++11 TestCluster.cpp libredisclient.a hiredis/libhiredis.a

ECHO:
	@echo $(SUBDIRS)
	@echo begin compile

RedisClusterClient.o: RedisClusterClient.cpp Mutex.hpp RedisClusterClient.h Util.cpp Util.h
#	$(CC) -c -std=c++11 RedisClusterClient.cpp
RedisClusterClient_handle.o: RedisClusterClient_handle.cpp Util.cpp Util.h
#	$(CC) -c -std=c++11 RedisClusterClient_handle.cpp
Util.o: Util.cpp Util.h
#	$(CC) -c -std=c++11 Util.cpp


$(SUBDIRS): ECHO
	+$(MAKE) -C $@

$(DYLIBNAME): $(OBJ)
	$(DYLIB_MAKE_CMD) -o $(DYLIBNAME) $(OBJ) $(REAL_LDFLAGS)

$(STLIBNAME): $(OBJ)
	$(STLIB_MAKE_CMD) $(STLIBNAME) $(OBJ)

$(TEST): $(STLIBNAME) $(SUBDIRS) TestCluster.cpp
	$(CC) -o $(TEST) -std=c++11 -pthread TestCluster.cpp $(STLIBNAME) hiredis/libhiredis.a

.cpp.o:
	$(CC) -std=c++11 -c $(REAL_LDFLAGS) $<

clean:
	for dir in $(SUBDIRS);\
		do $(MAKE) -C $$dir clean||exit 1;\
	done
	rm -rf *.o *.a $(TEST)
