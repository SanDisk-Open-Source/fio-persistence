#-----------------------------------------------------------------------------
#  Copyright (c) 2016 Western Digital Corporation or its affiliates.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions are met:
#  * Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#  * Neither the name of the Western Digital Corp. nor the names of its contributors
#    may be used to endorse or promote products derived from this software
#    without specific prior written permission.
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
#  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
#  A PARTICULAR PURPOSE ARE DISCLAIMED.
#  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#  FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
#  OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
#  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
#  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
#  WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#-----------------------------------------------------------------------------

RM=rm
CXX=g++
MKDIR=mkdir
CPPFLAGS=-DLIBAIO_SUPPORTED
INC_DIRS=/usr/include/jsoncpp

LIBS=-ljsoncpp -lpthread -laio

OBJ=$(patsubst %.cpp,%.o,$(wildcard *.cpp))

BIN=bin
ODIR=$(BIN)/obj

OBJ_LOC=$(patsubst %,$(ODIR)/%,$(OBJ))

make_bin:=$(shell $(MKDIR) -p $(BIN))
make_obj:=$(shell $(MKDIR) -p $(ODIR))

$(ODIR)/%.o : %.cpp
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -I$(INC_DIRS) -c $< -o $@

fio-persistence : $(OBJ_LOC)
	$(CXX) -o $(BIN)/fio-persistence $(OBJ_LOC) $(LIBS)

.PHONY: clean

clean:
	$(RM) -rf $(BIN)
