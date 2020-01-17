@echo off

SET SQLITE3_OPTIONS=-C -DSQLITE_ENABLE_FTS3 -C -DSQLITE_ENABLE_FTS3_PARENTHESIS -C -DSQLITE_THREADSAFE=0
REM For now we just always build with the shipped sqlite3 version.
COPY /y sqlite3\sqlite3.h .
COPY /y sqlite3\sqlite3ext.h .
COPY /y sqlite3\sqlite3.c sql3wrap.c
%CHICKEN_CSC% -C %CFLAGS% -L %LDFLAGS% %* -C -I. %SQLITE3_OPTIONS% -C -DSQLITE_THREADSAFE=0
