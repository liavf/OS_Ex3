# Compiler and flags
CC=g++
CXX=g++
LD=g++

INCS=-I.
CFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
CXXFLAGS = -Wall -std=c++11 -pthread -g $(INCS)
LDFLAGS = -L. -lMapReduceFramework

# Framework source and static library
FRAMEWORK_SRC = MapReduceFramework.cpp Barrier.cpp
FRAMEWORK_OBJ = $(FRAMEWORK_SRC:.cpp=.o)
FRAMEWORK_LIB = libMapReduceFramework.a

# Sample client executable
EXESRC = SampleClient.cpp
EXEOBJ = $(EXESRC:.cpp=.o)
EXE = SampleClient

# Final targets
TARGETS = $(EXE)

# Default rule
all: $(TARGETS)

# Link the SampleClient with the static framework
$(EXE): $(EXEOBJ) $(FRAMEWORK_LIB)
	$(LD) $(CXXFLAGS) $(EXEOBJ) $(FRAMEWORK_LIB) -o $(EXE)

# Create the static library from framework object files
$(FRAMEWORK_LIB): $(FRAMEWORK_OBJ)
	ar rcs $@ $^

# Compile .cpp to .o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Clean rule
clean:
	$(RM) $(TARGETS) $(EXEOBJ) $(FRAMEWORK_OBJ) $(FRAMEWORK_LIB) *~ *core

# Optional tar target
TAR = tar
TARFLAGS = -cvf
TARNAME = sampleclient.tar
TARSRCS = $(EXESRC) MapReduceFramework.cpp Barrier.cpp Makefile

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)