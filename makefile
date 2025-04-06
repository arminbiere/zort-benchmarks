all: zort
zort: zort.c makefile
	gcc -Wall -ggdb3 -o $@ $<
clean:
	rm -f zort
.PHONY: all clean
