/*
 * SQLite module to define virtual tables and table-valued functions natively using SQL.
 * In the interest of compatibility with SQLite's own license (or rather lack thereof),
 * the author disclaims copyright to this source code.
 */

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <limits.h>

/***************************************************************************
 *********************** MODIFICATIONS START HERE **************************
 *
 * This has been modified to cache prepared statements per vTab rather than
 * preparing in xOpen and finalizing in xClose.
 *
 * All modifications are conditionally included based on CACHE_STATEMENTS
 *
 * Change the following line to revert to the original.
 */

#define CACHE_STATEMENTS 1

#if CACHE_STATEMENTS
/*
 * Caching is optional, and controlled at runtime, defaulting to off.
 *
 * An additional API is available to turn it on, or off.
 */

void statementvtab_enable_cache (sqlite3*, int bOnOrOff);

/* Closing the connection with cached statements there may cause problems.
 * In theory, sqlite_close_v2 should still result in the virtual table
 * being closed, which in turn will clear the cache.
 *
 * But it is good practice to turn the cache off, which will clear it,
 * before closing a connection.
 */

#include <stdlib.h>

#ifdef DEBUG_TRACE
	#define TRACE(fmt, ...) printf("[STMT] " fmt "\n", ##__VA_ARGS__)
#else
	#define TRACE(fmt, ...) ((void)0)
#endif

#ifdef DEBUG_ASSERT
  #define ASSERT(expr) \
    do { \
      if( !(expr) ) { \
        fprintf(stderr, "[ASSERT] %s:%d: %s\n", __FILE__, __LINE__, #expr); \
        abort(); \
      } \
    } while (0)
#else
  #define ASSERT(expr) ((void)0)
#endif

/* forward typedefs of structures */
typedef struct CacheEntry CacheEntry;
typedef struct Connection Connection;
typedef struct statement_vtab statement_vtab;

/*
 * The cached statements look like this
 */
struct CacheEntry {
	statement_vtab *vtab;
	sqlite3_stmt *stmt;
	int bInUse;
	CacheEntry *next;
};

struct Connection {
	int bCache;
	statement_vtab *first;
};

#define CLIENTKEY "statement"

#endif /* CACHE_STATEMENTS */

struct statement_vtab {
	sqlite3_vtab base;
	sqlite3* db;
	char* sql;
	size_t sql_len;
	int num_inputs;
	int num_outputs;
#ifdef CACHE_STATEMENTS
	int nUsed;
	int nNodes;
	CacheEntry *first;
	statement_vtab *next;
#endif
};

struct statement_cursor {
	sqlite3_vtab_cursor base;
	sqlite3_stmt* stmt;
	int rowid;
	int param_argc;
	sqlite3_value** param_argv;
#ifdef CACHE_STATEMENTS
	/* Each cursor will have checked out one of the cached statements */
	CacheEntry *node;
#endif
};

/***************************************************************************
 ***************************************************************************
 *
 * These are the main routines of the changes to cache statements. Calls
 * to these are peppered through the original code with #ifdef guards
 *
 */
#ifdef CACHE_STATEMENTS

static Connection *connection_get(sqlite3 *db, int bCreate) {
	Connection *conn;
	conn = (Connection *)sqlite3_get_clientdata(db, CLIENTKEY);
	if( conn || !bCreate ) return conn;

	conn = sqlite3_malloc(sizeof(Connection));
	if( !conn ) return NULL;
	memset(conn, 0, sizeof(*conn));
	sqlite3_set_clientdata(db, CLIENTKEY, conn, sqlite3_free);
	return conn;
}

static void vtab_clear_cache(statement_vtab *vtab) {
	CacheEntry *node, *next;

	ASSERT( vtab );
	ASSERT( (vtab->nNodes == 0) == (vtab->first == NULL) );
	ASSERT( vtab->nUsed==0 );

	if( !vtab->nNodes ) return;

	TRACE("vtab_clear_cache");

	node = vtab->first;
	while( node ) {
		next = node->next;
		if( node->stmt ) {
			TRACE("vtab_clear_cache: finalize");
			sqlite3_finalize(node->stmt);
		}
		vtab->nNodes--;
		sqlite3_free(node);
		node = next;
	}
	vtab->first = NULL;
	ASSERT( vtab->nNodes == 0 );
}

static int cache_entry_get(statement_vtab *vtab, CacheEntry **pNode) {
	Connection *conn;
	CacheEntry *node;

	ASSERT( vtab );
	ASSERT( vtab->db );
	conn = connection_get(vtab->db, 1);
	if( !conn ) return SQLITE_NOMEM;

	for( node=vtab->first; node; node=node->next ) {
		if( !node->bInUse ) break;
	}

	if( !node ) {
		TRACE("cache_entry_get: new node");
		node = sqlite3_malloc64(sizeof(*node));
		if( !node ) return SQLITE_NOMEM;
		memset(node, 0, sizeof(*node));
		node->vtab = vtab;
		node->next = vtab->first;
		vtab->first = node;
		vtab->nNodes++;
	}

	if( !node->stmt ) {
		int rc;
		rc = sqlite3_prepare_v3(
			vtab->db,
			vtab->sql,
			vtab->sql_len,
			conn->bCache ? SQLITE_PREPARE_PERSISTENT : 0,
			&node->stmt,
			NULL
		);
		if( rc != SQLITE_OK ) {
			sqlite3_finalize(node->stmt);
			node->stmt = NULL;
			return rc;
		}
		TRACE("cache_entry_get: prepare");
	}

	node->bInUse = 1;
	vtab->nUsed++;
	*pNode = node;
	return SQLITE_OK;
}

void cache_entry_release(CacheEntry *node) {
	Connection *conn;

	ASSERT(node);
	ASSERT(node->vtab);
	ASSERT(node->vtab->db);
	ASSERT(node->bInUse == 1);
	ASSERT(node->stmt);

	conn = connection_get(node->vtab->db, 1);

	node->bInUse = 0;
	node->vtab->nUsed--;
	if( !conn || !conn->bCache ) {
		TRACE("cache_entry_release: finalize");
		sqlite3_finalize(node->stmt);
		node->stmt = NULL;
	} else {
		sqlite3_reset(node->stmt);
		sqlite3_clear_bindings(node->stmt);
	}
}
static void connection_clear_all_caches(Connection *conn) {
	statement_vtab *vtab;

	ASSERT(conn);
	TRACE("connection_clear_all_caches");

	for( vtab=conn->first; vtab; vtab=vtab->next ) {
		vtab_clear_cache(vtab);
	}
}

static void connection_add_vtab(statement_vtab *vtab) {
	Connection *conn;

	ASSERT(vtab->db);
	TRACE("connection_add_vtab");

	conn = connection_get(vtab->db, 1);
	if( !conn ) return;
	vtab->next = conn->first;
	conn->first = vtab;
}

static void connection_remove_vtab(statement_vtab *vtab) {
	Connection *conn;
	statement_vtab **pp;

	ASSERT(vtab);
	ASSERT(vtab->db);

	conn = connection_get(vtab->db, 0);
	if( !conn ) return;
	TRACE("connection_remove_vtab");

	for( pp=&conn->first; *pp; pp=&((*pp)->next) ) {
		if( *pp == vtab ) {
			*pp = vtab->next;
			break;
		}
	}
}


void statementvtab_enable_cache(sqlite3 *db, int nOnOff) {
	Connection *conn;
	ASSERT(db);

	conn = connection_get(db, 1);
	if( !conn ) return;

	conn->bCache = !!nOnOff;
	if( !conn->bCache ) connection_clear_all_caches(conn);
}

static void enable_cache_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
	sqlite3 *db;
	Connection *conn;

	db = sqlite3_context_db_handle(ctx);
	ASSERT(db);
	conn = connection_get(db, 1);
	if( !conn ) {
		sqlite3_result_error_nomem(ctx);
		return;
	}
	int eType = sqlite3_value_type(argv[0]);
	switch (eType) {
		case SQLITE_FLOAT:
		case SQLITE_INTEGER: {
			conn->bCache = (sqlite3_value_int(argv[0]) != 0);
			sqlite3_result_int(ctx, conn->bCache);
			if( !conn->bCache ) connection_clear_all_caches(conn);
			break;
		}
		case SQLITE_NULL: {
			sqlite3_result_int(ctx, conn->bCache);
			break;
		}
		default: {
			sqlite3_result_null(ctx);
		}
	}
}


/***************************************************************************
 ***************************************************************************
 */

#endif /* CACHE_STATEMENTS */

static char* build_create_statement(sqlite3_stmt* stmt) {
	sqlite3_str* sql = sqlite3_str_new(NULL);
	sqlite3_str_appendall(sql,"CREATE TABLE x( ");
	for(int i = 0, nout = sqlite3_column_count(stmt); i < nout; i++) {
		const char* name = sqlite3_column_name(stmt,i);
		if(!name) {
			sqlite3_free(sqlite3_str_finish(sql));
			return NULL;
		}
		const char* type = NULL;
		if(!sqlite3_compileoption_used("OMIT_DECLTYPE"))
			type = sqlite3_column_decltype(stmt,i);
		sqlite3_str_appendf(sql,"%Q %s,",name,(type?type:""));
	}
	for(int i = 0, nargs = sqlite3_bind_parameter_count(stmt); i < nargs; i++) {
		const char* name = sqlite3_bind_parameter_name(stmt,i+1);
		if(name)
			sqlite3_str_appendf(sql,"%Q hidden,",name+1);
		else
			sqlite3_str_appendf(sql,"'%d' hidden,",i+1);
	}
	if(sqlite3_str_length(sql))
		sqlite3_str_value(sql)[sqlite3_str_length(sql)-1] = ')';
	return sqlite3_str_finish(sql);
}

static int statement_vtab_destroy(sqlite3_vtab* pVTab){
#ifdef CACHE_STATEMENTS
    /* Clear the cache for this vtab */
	statement_vtab *vtab = (statement_vtab*)pVTab;
	vtab_clear_cache(vtab);
	connection_remove_vtab(vtab);
#endif
	sqlite3_free(((struct statement_vtab*)pVTab)->sql);
	sqlite3_free(pVTab);
	return SQLITE_OK;
}

static int statement_vtab_create(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** ppVtab, char** pzErr) {
	size_t len;
	if(argc < 4 || (len = strlen(argv[3])) < 3) {
		if(!(*pzErr = sqlite3_mprintf("no statement provided")))
			return SQLITE_NOMEM;
		return SQLITE_MISUSE;
	}
	if(argv[3][0] != '(' || argv[3][len-1] != ')') {
		if(!(*pzErr = sqlite3_mprintf("statement must be parenthesized")))
			return SQLITE_NOMEM;
		return SQLITE_MISUSE;
	}

	int ret;
	sqlite3_stmt* stmt = NULL;
	char* create = NULL;
#ifdef CACHE_STATEMENTS
	CacheEntry *node = NULL;
#endif

	struct statement_vtab* vtab = sqlite3_malloc64(sizeof(*vtab));
	if(!vtab)
		return SQLITE_NOMEM;
	memset(vtab,0,sizeof(*vtab));
	*ppVtab = &vtab->base;

	vtab->db = db;
	vtab->sql_len = len-2;
	if(!(vtab->sql = sqlite3_mprintf("%.*s",vtab->sql_len,argv[3]+1))) {
		ret = SQLITE_NOMEM;
		goto error;
	}

#ifdef CACHE_STATEMENTS
	/* Remember this vtab. And get a fresh prepared statement */
	connection_add_vtab(vtab);
	if((ret = cache_entry_get(vtab, &node)) != SQLITE_OK) goto sqlite_error;
	stmt = node->stmt;
#else
	if((ret = sqlite3_prepare_v2(db,vtab->sql,vtab->sql_len,&stmt,NULL)) != SQLITE_OK)
		goto sqlite_error;
#endif
	
	if(!sqlite3_stmt_readonly(stmt)) {
		ret = SQLITE_ERROR;
		if(!(*pzErr = sqlite3_mprintf("Statement must be read only.")))
			ret = SQLITE_NOMEM;
		goto error;
	}

	vtab->num_inputs = sqlite3_bind_parameter_count(stmt);
	vtab->num_outputs = sqlite3_column_count(stmt);

	if(!(create = build_create_statement(stmt))) {
		ret = SQLITE_NOMEM;
		goto error;
	}
	if((ret = sqlite3_declare_vtab(db,create)) != SQLITE_OK)
		goto sqlite_error;

	sqlite3_free(create);
#ifdef CACHE_STATEMENTS
	/* relese the statement back to the cache */
	cache_entry_release(node);
#else
	sqlite3_finalize(stmt);
#endif
	return SQLITE_OK;

sqlite_error:
	if(!(*pzErr = sqlite3_mprintf("%s",sqlite3_errmsg(db))))
		ret = SQLITE_NOMEM;
error:
	sqlite3_free(create);
#ifdef CACHE_STATEMENTS
	/* mark node as unused. xDestroy will then clean eveything up */
	cache_entry_release(node);
#else
	sqlite3_finalize(stmt);
#endif
	statement_vtab_destroy(*ppVtab);
	*ppVtab = NULL;
	return ret;
}

// if these point to the literal same function sqlite makes statement_vtab eponymous, which we don't want
static int statement_vtab_connect(sqlite3* db, void* pAux, int argc, const char* const* argv, sqlite3_vtab** ppVtab, char** pzErr) {
	return statement_vtab_create(db,pAux,argc,argv,ppVtab,pzErr);
}

static int statement_vtab_open(sqlite3_vtab* pVTab, sqlite3_vtab_cursor** ppCursor) {
	struct statement_vtab* vtab = (struct statement_vtab*)pVTab;
	struct statement_cursor* cur = sqlite3_malloc64(sizeof(*cur));
	if(!cur)
		return SQLITE_NOMEM;

	*ppCursor = &cur->base;
	cur->stmt = NULL;
	cur->param_argv = NULL;
#ifdef CACHE_STATEMENTS
	cur->node = NULL;
#endif

	if(vtab->num_inputs && !(cur->param_argv = sqlite3_malloc64(sizeof(*cur->param_argv)*vtab->num_inputs)))
		return SQLITE_NOMEM;

#ifdef CACHE_STATEMENTS
	int rc;
	if( (rc = cache_entry_get(vtab, &cur->node)) == SQLITE_OK ) {
		cur->stmt = cur->node->stmt;
	}
	return rc;
#else
	return sqlite3_prepare_v2(vtab->db,vtab->sql,vtab->sql_len,&cur->stmt,NULL);
#endif
}

static int statement_vtab_close(sqlite3_vtab_cursor* cur){
	struct statement_cursor* stmtcur = (struct statement_cursor*)cur;
#ifdef CACHE_STATEMENTS
	if (stmtcur->node) cache_entry_release(stmtcur->node);
#else
	sqlite3_finalize(stmtcur->stmt);
#endif
	sqlite3_free(stmtcur->param_argv);
	sqlite3_free(cur);
	return SQLITE_OK;
}

static int statement_vtab_next(sqlite3_vtab_cursor* cur){
	struct statement_cursor* stmtcur = (struct statement_cursor*)cur;
	int ret = sqlite3_step(stmtcur->stmt);
	if(ret == SQLITE_ROW) {
		stmtcur->rowid++;
		return SQLITE_OK;
	}
	return ret == SQLITE_DONE ? SQLITE_OK : ret;
}

static int statement_vtab_rowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid) {
	*pRowid = ((struct statement_cursor*)cur)->rowid;
	return SQLITE_OK;
}

static int statement_vtab_eof(sqlite3_vtab_cursor* cur) {
	return !sqlite3_stmt_busy(((struct statement_cursor*)cur)->stmt);
}

static int statement_vtab_column(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i) {
	struct statement_cursor* stmtcur = (struct statement_cursor*)cur;
	struct statement_vtab* vtab = (struct statement_vtab*)cur->pVtab;
	int num_outputs = vtab->num_outputs;
	int num_inputs = vtab->num_inputs;

	sqlite3_value* v;
	if(i < num_outputs) // a result from the statement
		v = sqlite3_column_value(stmtcur->stmt,i);
	else if(i-num_outputs < num_inputs) // one of the input parameters
		v = stmtcur->param_argv[i-num_outputs];
	else
		return SQLITE_RANGE;

	if(v)
		sqlite3_result_value(ctx,v);

	return SQLITE_OK;
}

// parameter map encoding for xBestIndex/xFilter
// constraint -> param index mappings are stored in idxStr when not contiguous. idxStr is expected to be NUL terminated
// and printable, so we use a 6 bit encoding in the ASCII range.
// for simplicity encoded indexes are fixed to the length necessary to encode an int. this is overkill on most systems
// due to sqlite's current hard limit on number of columns but makes statement_vtab agnostic to changes to this limit
const static size_t param_idx_size = (sizeof(int)*CHAR_BIT+5)/6;

static inline void encode_param_idx(int i, char* restrict param_map, int param_idx) {
	for(size_t j = 0; j < param_idx_size; j++)
		param_map[i*param_idx_size+j] = ((param_idx >> 6*j) & 63) + 33;
}

static inline int decode_param_idx(int i, const char* param_map) {
	int param_idx = 0;
	for(size_t j = 0; j < param_idx_size; j++)
		param_idx |= (param_map[i*param_idx_size+j] - 33) << 6*j;
	return param_idx;
}

// xBestIndex needs to communicate which columns are constrained by the where clause to xFilter;
// in terms of a statement table this translates to which parameters will be available to bind.
static int statement_vtab_filter(sqlite3_vtab_cursor* cur, int idxNum, const char* idxStr, int argc, sqlite3_value** argv) {
	struct statement_cursor* stmtcur = (struct statement_cursor*)cur;
	struct statement_vtab* vtab = (struct statement_vtab*)cur->pVtab;

	stmtcur->rowid = 1;
	sqlite3_stmt* stmt = stmtcur->stmt;
	sqlite3_reset(stmt);
	sqlite3_clear_bindings(stmt);
	if(vtab->num_inputs)
		memset(stmtcur->param_argv,0,sizeof(*stmtcur->param_argv)*vtab->num_inputs);

	int ret;
	for(int i = 0; i < argc; i++) {
		int param_idx = idxStr ? decode_param_idx(i,idxStr) : i+1;
		if((ret = sqlite3_bind_value(stmt,param_idx,argv[i])) != SQLITE_OK)
			return ret;
		// shallow copy args as these are explicitly retained in sqlite3WhereCodeOneLoopStart
		stmtcur->param_argv[param_idx-1] = argv[i];
	}
	ret = sqlite3_step(stmt);
	if(!(ret == SQLITE_ROW || ret == SQLITE_DONE))
		return ret;

	return SQLITE_OK;
}

static int statement_vtab_best_index(sqlite3_vtab* pVTab, sqlite3_index_info* index_info){
	struct statement_vtab* vtab = (struct statement_vtab*)pVTab;
	int num_outputs = vtab->num_outputs;
	int out_constraints = 0;
	index_info->orderByConsumed = 0;
	index_info->estimatedCost = 1;
	index_info->estimatedRows = 1;

	// avoid searching for input columns to bind to parameters if colUsed indicates this query has none
	// for tables with more than 63 columns the high bit indicates that one or more of these is set; in that case
	// there still may be no params but we need to continue on to check the constraint array
	if(!(index_info->colUsed >> (num_outputs < 63 ? num_outputs : 63)))
		return SQLITE_OK;

	int col_max = 0;
	sqlite3_uint64 used_cols = 0;
	for(int i = 0; i < index_info->nConstraint; i++) {
		// skip if this is a limit/offset constraint or a constraint on one of our output columns
		if(index_info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_LIMIT  ||
		   index_info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_OFFSET ||
		   index_info->aConstraint[i].iColumn < num_outputs)
			continue;
		// only select query plans where the constrained columns have exact values to bind to statement parameters
		// since the alternative requires scanning all possible results from the vtab
		if(!index_info->aConstraint[i].usable || index_info->aConstraint[i].op != SQLITE_INDEX_CONSTRAINT_EQ)
			return SQLITE_CONSTRAINT;

		int col_index = index_info->aConstraint[i].iColumn - num_outputs;
		index_info->aConstraintUsage[i].argvIndex = col_index+1;
		index_info->aConstraintUsage[i].omit = 1;

		if(col_index+1 > col_max)
			col_max = col_index+1;
		if(col_index < 64)
			used_cols |= 1ull << col_index;

		out_constraints++;
	}

	if(vtab->num_inputs < col_max)
		return SQLITE_RANGE;

	// if the constrained columns are contiguous then we can just tell sqlite to order the arg vector provided to xFilter
	// in the same order as our column bindings, so there's no need to map between these
	// (this will always be the case when calling the vtab as a table-valued function)
	// only support this optimization for up to 64 constrained columns since checking for continuity more generally would cost nearly as much
	// as just allocating the mapping
	sqlite_uint64 required_cols = (col_max < 64 ? 1ull << col_max : 0ull)-1;
	if(!out_constraints || (col_max <= 64 && used_cols == required_cols && out_constraints == col_max))
		return SQLITE_OK;

	// otherwise map the constraint index as provided to xFilter to column index for bindings
	// this will only be necessary when constraints are not contiguous e.g. where arg1 = x and arg3 = y
	// in that case bound parameter indexes are encoded as a string in idxStr, in the order they appear in constriants
	if((size_t)out_constraints > (SIZE_MAX-1)/param_idx_size) {
		sqlite3_free(pVTab->zErrMsg);
		if(!(pVTab->zErrMsg = sqlite3_mprintf("Too many constraints to index: %d",out_constraints)))
			return SQLITE_NOMEM;
		return SQLITE_ERROR;
	}

	if(!(index_info->idxStr = sqlite3_malloc64(out_constraints*param_idx_size+1)))
		return SQLITE_NOMEM;

	index_info->needToFreeIdxStr = 1;

	for(int i = 0, constraint_idx = 0; i < index_info->nConstraint; i++) {
		if(!index_info->aConstraintUsage[i].argvIndex)
			continue;
		encode_param_idx(constraint_idx,index_info->idxStr,index_info->aConstraintUsage[i].argvIndex);
		index_info->aConstraintUsage[i].argvIndex = ++constraint_idx;
	}

	index_info->idxStr[out_constraints*param_idx_size] = '\0';

	return SQLITE_OK;
}

static sqlite3_module statement_vtab_module = {
	.xCreate     = statement_vtab_create,
	.xConnect    = statement_vtab_connect,
	.xBestIndex  = statement_vtab_best_index,
	.xDisconnect = statement_vtab_destroy,
	.xDestroy    = statement_vtab_destroy,
	.xOpen       = statement_vtab_open,
	.xClose      = statement_vtab_close,
	.xFilter     = statement_vtab_filter,
	.xNext       = statement_vtab_next,
	.xEof        = statement_vtab_eof,
	.xColumn     = statement_vtab_column,
	.xRowid      = statement_vtab_rowid,
};

#ifdef SQLITE_CORE
#define statement_vtab_entry_point sqlite3_statementvtab_init
#else
#define statement_vtab_entry_point sqlite3_extension_init
#endif

#ifdef _WIN32
__declspec(dllexport)
#endif
int statement_vtab_entry_point(sqlite3* db, char** pzErrMsg, const sqlite3_api_routines* pApi) {
	SQLITE_EXTENSION_INIT2(pApi);

	if(sqlite3_libversion_number() < 3024000) {
		const char errmsg[] = "SQLite versions below 3.24.0 are not supported";
		if(pzErrMsg && (*pzErrMsg = sqlite3_malloc(sizeof(errmsg))))
			memcpy(*pzErrMsg, errmsg, sizeof(errmsg));
		return SQLITE_ERROR;
	}
#if CACHE_STATEMENTS
	sqlite3_create_function(db, "statement_enable_cache", 1, SQLITE_UTF8, NULL, enable_cache_func, NULL, NULL);
#endif
	return sqlite3_create_module(db, "statement", &statement_vtab_module, NULL);

}
