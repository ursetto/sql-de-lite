/* NOTE: pointers are not being checked for NULL. */

int sqlite3_close(sqlite3 *);
int sqlite3_exec(
  sqlite3*,                                  /* An open database */
  const char *sql,                           /* SQL to be evaluated */
  int (*callback)(void*,int,char**,char**),  /* Callback function */
  void *,                                    /* 1st argument to callback */
  char **errmsg                              /* Error msg written here */
);
int sqlite3_open(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite3 **ppDb          /* OUT: SQLite db handle */
);
int sqlite3_errcode(sqlite3 *db);
const char *sqlite3_errmsg(sqlite3 *db);
int sqlite3_extended_errcode(sqlite3 *db);   /* >= 3.6.5 */
int sqlite3_prepare_v2(
  sqlite3 *db,            /* Database handle */
  const char *zSql,       /* SQL statement, UTF-8 encoded */
  int nByte,              /* Maximum length of zSql in bytes. */
  sqlite3_stmt **ppStmt,  /* OUT: Statement handle */
  const char **pzTail     /* OUT: Pointer to unused portion of zSql */
);
sqlite3_stmt *sqlite3_next_stmt(sqlite3 *pDb, sqlite3_stmt *pStmt);
int sqlite3_step(sqlite3_stmt *);
int sqlite3_reset(sqlite3_stmt *pStmt);
int sqlite3_finalize(sqlite3_stmt *pStmt);
int sqlite3_changes(sqlite3*);
int sqlite3_total_changes(sqlite3*);
/* Max safe row on 32-bit is 2^53-1 (9007199254740992), on 64-bit is 2^62-1 */
int64_t sqlite3_last_insert_rowid(sqlite3*);
const char *sqlite3_sql(sqlite3_stmt *pStmt);

/* Binding */
int sqlite3_bind_parameter_count(sqlite3_stmt*);
int sqlite3_bind_parameter_index(sqlite3_stmt*, const char *zName);
const char *sqlite3_bind_parameter_name(sqlite3_stmt*, int);
int sqlite3_bind_blob(sqlite3_stmt*, int, ___byte_vector b,
                      int n, void(*)(void*));
int sqlite3_bind_double(sqlite3_stmt*, int, double);
int sqlite3_bind_int(sqlite3_stmt*, int, int);
/*int sqlite3_bind_int64(sqlite3_stmt*, int, sqlite3_int64);*/
int sqlite3_bind_null(sqlite3_stmt*, int);
int sqlite3_bind_text(sqlite3_stmt*, int, const char*, int n, void(*)(void*));
int sqlite3_bind_zeroblob(sqlite3_stmt*, int, int n);

/* query results */
int sqlite3_column_count(sqlite3_stmt *pStmt);
const void *sqlite3_column_blob(sqlite3_stmt*, int iCol);
int sqlite3_column_bytes(sqlite3_stmt*, int iCol);
double sqlite3_column_double(sqlite3_stmt*, int iCol);
int sqlite3_column_int(sqlite3_stmt*, int iCol);
/* Only returns 53 (or 62) significant bits, plus sign bit */
int64_t sqlite3_column_int64(sqlite3_stmt*, int iCol);
const /*unsigned*/ char *sqlite3_column_text(sqlite3_stmt*, int iCol);
int sqlite3_column_type(sqlite3_stmt*, int iCol);
const char *sqlite3_column_name(sqlite3_stmt*, int N);

