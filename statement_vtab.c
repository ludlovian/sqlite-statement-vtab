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

/*
 * This has been modified to cache prepared statements per vTab rather than
 * preparing in xOpen and finalizing in xClose.
 *
 * An additional API is available to clear the cache for a connection
 * prior to closing it. Although *probably* just calling sqlite3_close_v2
 * means this would still be okay without this, people should really
 * clean up their own mess.
 *
 *     void statementvtab_clear_cache(sqlite*)
 *
 * These change only appear if the code is compiled with CACHE_STATEMENTS
 *
 * This additional logic is not re-entrant. There are no mutexes.
 * Either use in single threads, or apply synchronisation outside.
 */

#ifdef CACHE_STATEMENTS

#define DEBUG 0
#if DEBUG
	#define DEBUG_LOG(fmt, ...) printf("[STMT] " fmt "\n", ##__VA_ARGS__)
#else
	#define DEBUG_LOG(fmt, ...) ((void)0)
#endif

/* forward typedefs of structures */
typedef struct stmt_cache_node stmt_cache_node;
typedef struct statement_vtab statement_vtab;

/*
 * The cache is a linked list of these nodes
 */
struct stmt_cache_node {
	sqlite3_stmt *stmt;
	int in_use;
	stmt_cache_node *next;
};

/*
 * A global linked list of vtabs
 */

static statement_vtab *g_vtab_list = NULL;

#endif /* CACHE_STATEMENTS */

struct statement_vtab {
	sqlite3_vtab base;
	sqlite3* db;
	char* sql;
	size_t sql_len;
	int num_inputs;
	int num_outputs;
#ifdef CACHE_STATEMENTS
	/* Each vtab has a cache of statements */
	stmt_cache_node *cache_head;
	/* Each vtab links to the next */
	statement_vtab *next_vtab;
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
	stmt_cache_node *node;
#endif
};

#ifdef CACHE_STATEMENTS

static void add_vtab_to_global_list(statement_vtab *vtab) {
	DEBUG_LOG("Add vtab %p", vtab);
	vtab->next_vtab = g_vtab_list;
	g_vtab_list = vtab;
}

static void remove_vtab_from_global_list(statement_vtab *vtab) {
	DEBUG_LOG("Remove vtab %p", vtab);
	statement_vtab **pp;
	for (pp = &g_vtab_list; *pp; pp = &((*pp)->next_vtab)) {
		if (*pp == vtab) {
			*pp = vtab->next_vtab;
			break;
		}
	}
}

static void clear_vtab_cache(statement_vtab *vtab) {
	if (vtab->cache_head) {
		DEBUG_LOG("Clear vtab cache %p", vtab);
		stmt_cache_node* node = vtab->cache_head;
		while (node) {
			stmt_cache_node *next = node->next;
			DEBUG_LOG("free node %p", node);
			if (node->stmt) sqlite3_finalize(node->stmt);
			sqlite3_free(node);
			node = next;
		}
		vtab->cache_head = NULL;
	}
}

/* The meat of the work. We fetch an existing unused statement from the
 * cache if we have one. If not, then we prepare a new statement and
 * add it to the cache
 */

static int get_or_create_node(statement_vtab *vtab, stmt_cache_node **pNode) {
	stmt_cache_node *node = vtab->cache_head;
	while (node) {
		if (!node->in_use) {
			node->in_use = 1;
			*pNode = node;
			DEBUG_LOG("Reuse node %p", node);
			return SQLITE_OK;
		}
		node = node->next;
	}

	node = sqlite3_malloc64(sizeof(*node));
	if (!node) return SQLITE_NOMEM;

	int rc = sqlite3_prepare_v3(vtab->db, vtab->sql, vtab->sql_len, 
								SQLITE_PREPARE_PERSISTENT, &node->stmt, NULL);
	if (rc != SQLITE_OK) {
		sqlite3_free(node);
		return rc;
	}

	node->in_use = 1;
	node->next = vtab->cache_head;
	vtab->cache_head = node;
	*pNode = node;
	DEBUG_LOG("Create node %p for vtab %p", node, vtab);
	return SQLITE_OK;
}

/* Once we have finished with a statement, we release it back to the cache
 * for reuse rather than finalizing it. Statements are reset and bindings
 * cleared before releasing it.
 */
static void release_stmt(stmt_cache_node *node) {
	DEBUG_LOG("Release node %p", node);
	node->in_use = 0;
	if (node->stmt) {
		sqlite3_reset(node->stmt);
		sqlite3_clear_bindings(node->stmt);
	}
}

/* The external API to be used to clear all cached statements for
 * a given DB connection
 */

void statementvtab_clear_cache(sqlite3 *db) {
	DEBUG_LOG("Clearing all for conn %p", db);
	statement_vtab* p = g_vtab_list;
	while (p) {
		if (p->db == db) clear_vtab_cache(p);
		p = p->next_vtab;
	}
}

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
	DEBUG_LOG("vtab_destroy");
	clear_vtab_cache(vtab);
	remove_vtab_from_global_list(vtab);
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
	stmt_cache_node *node = NULL;
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
	add_vtab_to_global_list(vtab);
	if((ret = get_or_create_node(vtab, &node)) != SQLITE_OK)
		goto sqlite_error;
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
	release_stmt(node);
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
	release_stmt(node);
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
	if((rc = get_or_create_node(vtab, &cur->node)) == SQLITE_OK)
		cur->stmt = cur->node->stmt;
	return rc;
#else
	return sqlite3_prepare_v2(vtab->db,vtab->sql,vtab->sql_len,&cur->stmt,NULL);
#endif
}

static int statement_vtab_close(sqlite3_vtab_cursor* cur){
	struct statement_cursor* stmtcur = (struct statement_cursor*)cur;
#ifdef CACHE_STATEMENTS
	if (stmtcur->node) release_stmt(stmtcur->node);
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

	return sqlite3_create_module(db, "statement", &statement_vtab_module, NULL);
}
