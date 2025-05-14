CC = g++
CXX = g++
RANLIB = ranlib
ARFLAGS = rcs

LIBSRC = Barrier.cpp MapReduceFramework.cpp
LIBOBJ = $(LIBSRC:.cpp=.o)

INCS = -I.
CFLAGS = -Wall -std=c++11 -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -g $(INCS)

MAPREDUCELIB = libMapReduceFramework.a
TARGETS = $(MAPREDUCELIB)

TAR = tar
TARFLAGS = -cvf
TARNAME = ex3.tar
TARSRCS = $(LIBSRC) Barrier.h Makefile README

TESTDIR = tests
TESTSOURCES := $(wildcard $(TESTDIR)/*.cpp)
TESTBINS := $(patsubst $(TESTDIR)/%.cpp, $(TESTDIR)/%, $(TESTSOURCES))

all: $(TARGETS)

$(TARGETS): $(LIBOBJ)
	$(AR) $(ARFLAGS) $@ $^
	$(RANLIB) $@

test: $(MAPREDUCELIB) $(TESTBINS)
	@echo "Running tests..."
	@mkdir -p actual
	@bash -c 'for bin in $(TESTBINS); do \
		name=$$(basename $$bin); \
		echo "Running $$bin..."; \
		./$$bin > actual/$$name.out 2>&1; \
		if [ -f $$bin.txt ]; then \
			if diff -u $$bin.txt actual/$$name.out > actual/$$name.diff; then \
				echo "✅ $$bin passed"; \
				rm -f actual/$$name.diff; \
			else \
				echo "❌ $$bin failed (mismatch):"; \
				echo "------------------- DIFF -------------------"; \
				cat actual/$$name.diff; \
				echo "---------------------------------------------"; \
			fi; \
		else \
			echo "⚠️  No expected output ($$bin.txt) to compare."; \
		fi; \
	done'

$(TESTDIR)/%: $(TESTDIR)/%.cpp $(MAPREDUCELIB)
	$(CXX) $(CXXFLAGS) -o $@ $< -L. -lMapReduceFramework -lpthread
clean:
	$(RM) $(TARGETS) $(LIBOBJ) *~ *core

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)