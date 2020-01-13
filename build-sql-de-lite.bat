@echo off

SET SQLITE3_OPTIONS=-C -DSQLITE_ENABLE_FTS3 -C -DSQLITE_ENABLE_FTS3_PARENTHESIS -C -DSQLITE_THREADSAFE=0

%CHICKEN_CSI% -s fixup.scm -s %* > static.tmp
SET /P STATIC=<static.tmp
%CHICKEN_CSI% -s fixup.scm -o %* > opts.tmp
SET /P OPTS=<opts.tmp

REM For now we just always build with the shipped sqlite3 version.
IF NOT EXISTS sqlite3\sqlite3%STATIC%.o %CHICKEN_CSC% %OPTS% -C %CFLAGS% -L %LDFLAGS% -c -Isqlite3 sqlite3/sqlite3.c %SQLITE3_OPTIONS% -o sqlite3/sqlite3%STATIC%.o
%CHICKEN_CSC% -C %CFLAGS% -L %LDFLAGS% %* -Isqlite3 sqlite3/sqlite3.c %SQLITE3_OPTIONS% -C -DSQLITE_THREADSAFE=0 sqlite3/sqlite3%STATIC%.o
