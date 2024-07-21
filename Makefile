# CXX := g++-7
CXX := g++
CXXFLAGS := -std=c++14 -Wall -g -pedantic-errors -Werror -O3 \
						-Wno-unused-parameter -Wimplicit-fallthrough=0 -Wextra -Weffc++
# REPLAY := yes enables a policy that can feed traces to memcached, but it
# requires libmemcached to be linked in as a result. Only enable it if you
# have the library and headers installed.
REPLAY ?= no

# LDFLAGS := -lcrypto -lssl -lpython2.7
LDFLAGS := -lcrypto -lssl -lpython3.10
# LDFLAGS := -lcrypto -lssl -L/usr/lib/python3.10/config-3.10-x86_64-linux-gnu -lpython3.10

ifeq ($(REPLAY),yes)
LDFLAGS += -lmemcached 
else
CXXFLAGS += -DNOREPLAY
endif

HEADERS := $(wildcard src/*.h)
SRCS := $(wildcard src/*.cpp)
OBJS := $(patsubst src/%.cpp, bin/%.o, $(SRCS))

all: bin/lsm-sim

# 确保bin目录存在
bin:
	mkdir -p bin

bin/%.o: src/%.cpp $(HEADERS) | bin
	$(CXX) $(CXXFLAGS) -c $< -o $@

bin/lsm-sim: $(OBJS)
	$(CXX) -o $@ $^ $(LDFLAGS)

clean:
	-rm bin/lsm-sim bin/*.o

debug: CXXFLAGS += -DDEBUG
debug: bin/lsm-sim
