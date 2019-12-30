
CXX=g++
CXXFLAGS=-O3 -Wextra -pthread --std=c++11

bdb: bdb.o 
	g++ $(CXXFLAGS) $? -o $@

example: bdb
	./bdb ~

clean:
	rm -f bdb *.o tags

tags:
	ctags *.cpp
