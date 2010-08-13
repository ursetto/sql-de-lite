# Pre-setup makefile for sql-de-lite.  Run manually.
# Generates foreign API bindings via the bind egg, avoiding
# install-time dependency on bind.

all: sqlite3-api.scm

sqlite3-api.scm: sqlite3-api.h
	chicken-bind $<
