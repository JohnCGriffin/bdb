
bdb: main.go
	go build -o $@

bdb-linux: main.go
	GOOS=linux go build -o $@

example: bdb
	./bdb ~

clean:
	rm -f bdb bdb-linux *.o tags

