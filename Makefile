CC          = $(CXX)
CXXFLAGS    = -O3 -Wall -Wextra -Wconversion
EXE         = ./join
OBJS        = Query.o Result.o join.o main.o test.o auxFun.o JobScheduler.o

all: $(EXE)

$(EXE): $(OBJS)

clean:
	rm -f $(OBJS) $(EXE)

run: all
	@cat small/small.init small/small.work | (time --format="Time Elapsed: %E" $(EXE))
