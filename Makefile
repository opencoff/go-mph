
srcs = $(wildcard *.go)
mphdb_srcs = $(wildcard example/*.go)

all: mphdb

mphdb: $(srcs) $(mphdb_srcs)
	go build -o $@ ./example


test: $(srcs)
	go test

.PHONY: clean realclean

clean realclean:
	-rm -f mphdb
