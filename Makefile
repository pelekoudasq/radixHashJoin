CC       = $(CXX)
CXXFLAGS = -O3 -Wall -Wextra -Wconversion -std=c++11
LDFLAGS  = -pthread
# EXE      = $(shell basename $(CURDIR))
EXE      = join
OBJS     = $(patsubst %.cpp, %.o, $(wildcard *.cpp))

$(EXE): $(OBJS)

clean:
	rm -f $(OBJS) $(EXE)

run: $(EXE)
	@cat small/small.init small/small.work | (time --format="Time Elapsed: %E" ./$(EXE))
