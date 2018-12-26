CC       = $(CXX)
CXXFLAGS = -O3 -Wall -Wextra -Wconversion
EXE      = $(shell basename $(CURDIR))
OBJS     = $(patsubst %.cpp, %.o, $(wildcard *.cpp))

$(EXE): $(OBJS)

clean:
	rm -f $(OBJS) $(EXE)

run: $(EXE)
	@cat small/small.init small/small.work | (time --format="Time Elapsed: %E" ./$(EXE))
