COMPILER=corral run -- ponyc
COMPILERFLAGS=
SRCS=$(shell find src -type f)
TARGET=main

# change the order of debug and release to change the default
debug: COMPILERFLAGS+=--debug
debug: main
release: main

main: _corral _repos $(SRCS)
	$(COMPILER) $(COMPILERFLAGS) src -b $(TARGET)
	@sudo setcap CAP_NET_BIND_SERVICE=+eip $(TARGET)
	
# ensure dependencies are present
_corral:
	corral fetch
_repos:
	corral fetch

.PHONY: clean all release debug
clean:
	rm -f $(TARGET)
