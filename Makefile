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
	@bash -c 'for bin in $(TESTBINS); do \
		echo "Running $$bin..."; \
		./$$bin | tee $$bin.out; \
		if [ -f $$bin.txt ]; then \
			if diff <(sort $$bin.out) <(sort $$bin.txt); then \
				echo "✅ $$bin passed"; \
			else \
				echo "❌ $$bin failed (sorted diff mismatch)"; \
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