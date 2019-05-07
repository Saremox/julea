/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2018 Michael Straßberger
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * \file
 **/

#include <julea-config.h>

#include <glib.h>
#include <glib/gprintf.h>

#include <gmodule.h>

#include <sqlite3.h>

#include <julea.h>
#include <julea-internal.h>
#include <julea-smd.h>

static sqlite3* backend_db = NULL;

// Temporary until Julea provides helper header files
// for this.
static
void _sqlite3_finalize(void * ptr)
{
	sqlite3_finalize((sqlite3_stmt*)ptr);
}

static
void _bson_destroy(void * ptr)
{
	bson_destroy(ptr);
	g_slice_free(bson_t,ptr);
}

typedef gint SMD_TRANSACTION;

static
void _transaction_begin(SMD_TRANSACTION* ptr)
{
	*ptr = 1;
	if (sqlite3_exec(backend_db, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_CRITICAL("cannot begin transaction: %s",sqlite3_errmsg(backend_db))
	}
}

static
void _transaction_end(SMD_TRANSACTION* ptr)
{
	if(*ptr == 1)
	{
		*ptr = 0;
		if (sqlite3_exec(backend_db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK)
		{
			J_CRITICAL("cannot end transaction: %s",sqlite3_errmsg(backend_db))
		}
	}
}

static
void _transaction_abort(SMD_TRANSACTION* ptr)
{
	if(*ptr == 1)
	{
		*ptr = 0;
		if (sqlite3_exec(backend_db, "ROLLBACK;", NULL, NULL, NULL) != SQLITE_OK)
		{
			J_CRITICAL("cannot end transaction: %s",sqlite3_errmsg(backend_db))
		}
	}
}

#define UN_CONSTIFY(_t, _v) ((_t)(uintptr_t)(_v)) 

G_DEFINE_AUTOPTR_CLEANUP_FUNC(bson_t, _bson_destroy)
G_DEFINE_AUTOPTR_CLEANUP_FUNC(sqlite3_stmt,_sqlite3_finalize)
G_DEFINE_AUTOPTR_CLEANUP_FUNC(SMD_TRANSACTION,_transaction_abort)

/*
 * Helper Functions
 */
static
int64_t 
smd_get_type(bson_iter_t* iter)
{
	return bson_iter_int64(iter);
}

static
gchar* 
generate_create_table_stmt(gchar const* namespace, bson_t const* scheme)
{
	bson_iter_t iter;
	GString* create_querry = g_string_new(NULL);
	g_string_append_printf(create_querry,
							"CREATE TABLE `%s` (`key` TEXT NOT NULL PRIMARY KEY,",
							namespace);
	if(! bson_iter_init(&iter, scheme))
	{
		J_CRITICAL("Wasnt able to create iterator for namespace %s scheme definition", namespace);
		return FALSE;
	}
	bson_iter_next(&iter);
	while(true)
	{
		int64_t jsmd_type = smd_get_type(&iter);

		switch(jsmd_type)
		{
			
			case JSMD_TYPE_INTEGER_8:
				g_string_append_printf(create_querry,
										"\n `%s` TINYINT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_INTEGER_16:
				g_string_append_printf(create_querry,
										"\n `%s` SMALLINT NOT NULL",
										bson_iter_key(&iter));
				break;
			
			case JSMD_TYPE_INTEGER_32:
				g_string_append_printf(create_querry,
											"\n `%s` INT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_INTEGER:
			case JSMD_TYPE_INTEGER_64:
				g_string_append_printf(create_querry,
										"\n `%s` BIGINT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_UNSIGNED_INTEGER_8:
			case JSMD_TYPE_UNSIGNED_INTEGER_16:
			case JSMD_TYPE_UNSIGNED_INTEGER_32:
				g_string_append_printf(create_querry,
										"\n `%s` INTEGER NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_TEXT:
				g_string_append_printf(create_querry,
										"\n `%s` TEXT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_FLOAT:
			case JSMD_TYPE_FLOAT_16:
			case JSMD_TYPE_FLOAT_32:
			case JSMD_TYPE_FLOAT_64:
				g_string_append_printf(create_querry,
										"\n `%s` REAL NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_DATE_TIME:
				g_string_append_printf(create_querry,
										"\n `%s` DATETIME NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_INTEGER_128:
			case JSMD_TYPE_UNSIGNED_INTEGER:
			case JSMD_TYPE_UNSIGNED_INTEGER_64:
			case JSMD_TYPE_UNSIGNED_INTEGER_128:
			case JSMD_TYPE_FLOAT_128:
			case JSMD_TYPE_FLOAT_256:
				g_string_append_printf(create_querry,
										"\n `%s` BLOB NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_UNKNOWN:
			default:
				J_WARNING("The type of column %s in namespace %s is not valid",bson_iter_key(&iter),namespace);
				return NULL;
		}

		if(bson_iter_next(&iter))
		{
			g_string_append_printf(create_querry,",");
			continue;
		}
		else
		{
			g_string_append_printf(create_querry,");");
			break;
		}
	}

	return g_string_free(create_querry,false);
}

static
gchar* 
generate_insert_table_stmt(gchar const* namespace, bson_t const* node)
{
	bson_iter_t iter_node;
	GString* insert_querry = g_string_new(NULL);
	GString* insert_querry_placeholder = g_string_new("?,");
	g_string_append_printf(insert_querry, "INSERT INTO `%s` (`key`,",namespace);

	if(!bson_iter_init(&iter_node,node))
	{
		return NULL;
	}

	bson_iter_next(&iter_node);
	while(true)
	{
		g_string_append_printf(insert_querry, "`%s`", bson_iter_key(&iter_node));
		g_string_append(insert_querry_placeholder,"?");
		if(bson_iter_next(&iter_node))
		{
			g_string_append(insert_querry,",");
			g_string_append(insert_querry_placeholder,",");
			continue;
		}
		else
		{
			g_autofree gchar* placeholder= g_string_free(insert_querry_placeholder,false);
			g_string_append_printf(insert_querry,") VALUES (%s);",placeholder);
			break;
		}
	}
	
	return g_string_free(insert_querry,false);
}

static
gchar* 
generate_update_table_stmt(gchar const* namespace, bson_t const* node)
{
	bson_iter_t iter_node;
	GString* update_querry;
	GString* column_names = g_string_new(NULL);
	GString* sql_bind = g_string_new(NULL);
	GString* upsert = g_string_new(NULL);

	if(!bson_iter_init(&iter_node,node))
	{
		return NULL;
	}

	bson_iter_next(&iter_node);
	while(true)
	{
		const gchar* column_name = bson_iter_key(&iter_node);
		g_string_append_printf(column_names, "%s", column_name);
		g_string_append(sql_bind,"?");
		g_string_append_printf(upsert,"`%s` = ?",column_name);
		if(!bson_iter_next(&iter_node))
		{
			g_autofree gchar* cname = g_string_free(column_names,false);
			g_autofree gchar* sbind = g_string_free(sql_bind,false);
			g_autofree gchar* usert = g_string_free(upsert,false);
			update_querry = g_string_new(NULL);
			g_string_append_printf(update_querry,"INSERT INTO `%s` (`key`,%s) VALUES (?, %s) ON CONFLICT(key) DO UPDATE SET %s;", namespace, cname, sbind, usert);
			
			return g_string_free(update_querry,false);
		}
		g_string_append(column_names,",");
		g_string_append(sql_bind,",");
		g_string_append(upsert,",");
	}

	
}

static 
gchar*
create_get_stmt(gchar const* namespace, gchar const* key)
{
	GString* get_querry = g_string_new(NULL);

	g_string_append_printf(get_querry,"SELECT * FROM `%s` WHERE `key` = '%s'", namespace, key);
	return g_string_free(get_querry,FALSE);
}

static 
gchar*
create_delete_stmt(gchar const* namespace, gchar const* key)
{
	GString* get_querry = g_string_new(NULL);

	g_string_append_printf(get_querry,"DELETE FROM `%s` WHERE `key` = '%s'", namespace, key);
	return g_string_free(get_querry,FALSE);
}


/* 
 * SMD API
 */

static
gboolean 
backend_init (gchar const* path)
{
	g_autofree gchar* dirname = NULL;

	g_return_val_if_fail(path != NULL, FALSE);

	dirname = g_path_get_dirname(path);
	g_mkdir_with_parents(dirname, 0700);

	J_DEBUG("Open SQLITE database on path - %s", path);

	if (sqlite3_open(path, &backend_db) != SQLITE_OK)
	{
		J_CRITICAL("Was not able to open Database on path %s : %s",path,sqlite3_errmsg(backend_db));
		return FALSE;
	}

	// Create namespace structure

	if (sqlite3_exec(backend_db, "CREATE TABLE IF NOT EXISTS _julea_structure_ (namespace TEXT NOT NULL, cached_scheme BLOB NOT NULL);", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_CRITICAL("failed to create julea_smd management structure: %s",sqlite3_errmsg(backend_db))
		return FALSE;
	}

	if (sqlite3_exec(backend_db, "CREATE UNIQUE INDEX IF NOT EXISTS _julea_structure_index_ ON _julea_structure_ (namespace);", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_CRITICAL("failed to create julea_smd management structure: %s",sqlite3_errmsg(backend_db))
		return FALSE;
	}

	return TRUE;
}

static
void 
backend_fini (void)
{
	// sqlite3_close handles NULL parameter as a no-op
	sqlite3_close(backend_db); 
	backend_db = NULL;
}

static
gboolean 
backend_apply_scheme (gchar const* namespace, bson_t const* scheme)
{
	g_autofree gchar* create_querry = NULL;
	g_autoptr(sqlite3_stmt) insert_scheme_cache = NULL;
	SMD_TRANSACTION t = 0;
	g_autoptr(SMD_TRANSACTION) transaction = &t;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(scheme != NULL, FALSE);

	_transaction_begin(transaction);

	// Create SQL Create Table statement from json document
	create_querry = generate_create_table_stmt(namespace,scheme);
	if(create_querry != NULL)
	{
		if(sqlite3_exec(backend_db, create_querry, NULL, NULL, NULL) != SQLITE_OK)
		{
			J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db))
			return FALSE;
		}
	}
	else
	{
		J_CRITICAL("create_querry was empty for %s",namespace)
		return FALSE;
	}

	// Insert into namespace and scheme cache
	if(sqlite3_prepare_v2(backend_db, "INSERT INTO _julea_structure_ (`namespace`, `cached_scheme`) VALUES (?, ?);", -1, &insert_scheme_cache, NULL) != SQLITE_OK)
	{
		J_WARNING("%s",sqlite3_errmsg(backend_db));
		return FALSE;
	}
	if(sqlite3_bind_text(insert_scheme_cache, 1, namespace, -1, NULL) != SQLITE_OK)
	{
		J_WARNING("%s",sqlite3_errmsg(backend_db));
		return FALSE;
	}
	if(sqlite3_bind_blob(insert_scheme_cache, 2, bson_get_data(scheme), scheme->len, NULL) != SQLITE_OK)
	{
		J_WARNING("%s",sqlite3_errmsg(backend_db));
		return FALSE;
	}
	if(sqlite3_step(insert_scheme_cache) != SQLITE_DONE)
	{
		J_WARNING("%s",sqlite3_errmsg(backend_db));
		return FALSE;
	}

	_transaction_end(transaction);

	return TRUE;
}

static
gboolean 
backend_get_scheme (gchar const* namespace, bson_t* scheme)
{
	g_autoptr(sqlite3_stmt) stmt = NULL;
	gint ret;
	gconstpointer result = NULL;
	gsize result_len;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(scheme != NULL, FALSE);

	if(sqlite3_prepare_v2(backend_db, "SELECT cached_scheme FROM _julea_structure_ WHERE namespace = ?;", -1, &stmt, NULL) != SQLITE_OK)
	{
		J_CRITICAL("Failed to prepare get scheme stmt for namespace %s", namespace);
	}
	sqlite3_bind_text(stmt, 1, namespace, -1, NULL);

	ret = sqlite3_step(stmt);

	if (ret == SQLITE_ROW)
	{
		bson_t tmp;
	
		result = sqlite3_column_blob(stmt, 0);
		result_len = sqlite3_column_bytes(stmt, 0);

		// FIXME check whether copies can be avoided
		// Sadly it's not possible to steal the pointer
		// of SQLITE
		bson_init_static(&tmp, result, result_len);
		bson_copy_to(&tmp, scheme);
		return TRUE;
	}
	else if(ret != SQLITE_DONE)
	{
		J_WARNING("SQL error : %s",sqlite3_errmsg(backend_db));
		return FALSE;
	}
	else
	{
		J_INFO("Namespace %s not found",namespace);
		return FALSE;
	}
}

static
gboolean
_backend_insert (gchar const* namespace, gchar const* key, bson_t const* node, gboolean allow_update)
{
	g_autoptr(sqlite3_stmt) insert_stmt = NULL;
	bson_t insert_scheme;
	bson_iter_t iter_node;
	bson_iter_t iter_scheme;
	g_autofree gchar* insert_querry = NULL;
	int64_t insert_index;
	int64_t num_keys_scheme;

	if(!backend_get_scheme(namespace, &insert_scheme))
	{
		J_WARNING("There is no scheme for this namespace %s",namespace);
		return FALSE;
	}

	if(allow_update)
	{
		insert_querry = generate_update_table_stmt(namespace,&insert_scheme);
	}
	else
	{
		insert_querry = generate_insert_table_stmt(namespace,&insert_scheme);
	}
	
	if(insert_querry != NULL)
	{
		if( sqlite3_prepare_v2(backend_db, insert_querry, -1, &insert_stmt, NULL) != SQLITE_OK)
		{
			J_CRITICAL("Failed to prepare insert statement \"%s\" : %s",insert_querry,sqlite3_errmsg(backend_db));
			return FALSE;
		}
	}
	
	if(!bson_iter_init(&iter_scheme,&insert_scheme))
	{
		J_CRITICAL("Failed to initialize bson iterator for node tree 0x%p", UN_CONSTIFY(gpointer,node));
		return FALSE;
	}
	insert_index = 2;
	sqlite3_bind_text(insert_stmt, 1, key,-1,NULL);
	num_keys_scheme = bson_count_keys(&insert_scheme);
	J_WARNING("%s",bson_as_json(node,NULL));
	while(bson_iter_next(&iter_scheme))
	{
		JSMD_TYPE jsmd_type;
		uint32_t bytes = 0; // Size of binary data;
		const uint8_t* blob;
		const gchar* cur_key = bson_iter_key(&iter_scheme);
		if(!bson_iter_init_find(&iter_node,node,cur_key))
		{
			continue;
		}
		jsmd_type = smd_get_type(&iter_scheme);
		switch(jsmd_type)
		{
			case JSMD_TYPE_INTEGER: __attribute__ ((fallthrough));
			case JSMD_TYPE_INTEGER_8: __attribute__ ((fallthrough));
			case JSMD_TYPE_INTEGER_16: __attribute__ ((fallthrough));
			case JSMD_TYPE_INTEGER_32: __attribute__ ((fallthrough));
			case JSMD_TYPE_INTEGER_64: __attribute__ ((fallthrough));
			case JSMD_TYPE_UNSIGNED_INTEGER_8: __attribute__ ((fallthrough));
			case JSMD_TYPE_UNSIGNED_INTEGER_16: __attribute__ ((fallthrough));
			case JSMD_TYPE_UNSIGNED_INTEGER_32:
				if( bson_iter_type(&iter_node) != BSON_TYPE_INT64)
				{
					J_INFO("The Type in bson_tree 0x%p in column %s is not %s", UN_CONSTIFY(gpointer,node),cur_key,j_smd_type_type2string(jsmd_type))
					return false;
				}
				sqlite3_bind_int64(insert_stmt, insert_index, bson_iter_as_int64(&iter_node));
				if(allow_update)
				{
					sqlite3_bind_int64(insert_stmt, insert_index + num_keys_scheme, bson_iter_as_int64(&iter_node));
				}
				break;
			case JSMD_TYPE_DATE_TIME: __attribute__ ((fallthrough));
			case JSMD_TYPE_TEXT:
				if( bson_iter_type(&iter_node) != BSON_TYPE_UTF8)
				{
					J_INFO("The Type in bson_tree 0x%p in column %s is not %s", UN_CONSTIFY(gpointer,node),cur_key,j_smd_type_type2string(jsmd_type))
					return false;
				}
				sqlite3_bind_text(insert_stmt, insert_index, bson_iter_utf8(&iter_node,NULL),-1,NULL);
				if(allow_update)
				{
					sqlite3_bind_text(insert_stmt, insert_index + num_keys_scheme, bson_iter_utf8(&iter_node,NULL),-1,NULL);
				}
				break;
			case JSMD_TYPE_FLOAT: __attribute__ ((fallthrough));
			case JSMD_TYPE_FLOAT_16: __attribute__ ((fallthrough));
			case JSMD_TYPE_FLOAT_32: __attribute__ ((fallthrough));
			case JSMD_TYPE_FLOAT_64:
				if( bson_iter_type(&iter_node) != BSON_TYPE_DOUBLE)
				{
					J_INFO("The Type in bson_tree 0x%p in column %s is not %s", UN_CONSTIFY(gpointer,node),cur_key,j_smd_type_type2string(jsmd_type))
					return false;
				}
				sqlite3_bind_double(insert_stmt, insert_index, bson_iter_as_double(&iter_node));
				if(allow_update)
				{
					sqlite3_bind_double(insert_stmt, insert_index + num_keys_scheme, bson_iter_as_double(&iter_node));
				}
				break;
			case JSMD_TYPE_FLOAT_256: 
				bytes += 16; __attribute__ ((fallthrough));// Size difference from 256bits to 128bits 
			case JSMD_TYPE_FLOAT_128: __attribute__ ((fallthrough));
			case JSMD_TYPE_INTEGER_128: __attribute__ ((fallthrough));
			case JSMD_TYPE_UNSIGNED_INTEGER_128:
				bytes += 8; __attribute__ ((fallthrough));// Size difference from 128bits to 64bits 
			case JSMD_TYPE_UNSIGNED_INTEGER: __attribute__ ((fallthrough));
			case JSMD_TYPE_UNSIGNED_INTEGER_64: 
				bytes += 8; // Size of 64 Bit unsigned integer
				if( bson_iter_type(&iter_node) != BSON_TYPE_BINARY)
				{
					J_INFO("The Type in bson_tree 0x%p in column %s is not %s", UN_CONSTIFY(gpointer,node),cur_key,j_smd_type_type2string(jsmd_type))
					return false;
				}
				
				bson_iter_binary(&iter_node, NULL, &bytes, &blob);
				sqlite3_bind_blob(insert_stmt, insert_index, &blob , bytes , NULL);
				if(allow_update)
				{
					sqlite3_bind_blob(insert_stmt, insert_index + num_keys_scheme, &blob , bytes , NULL);
				}
				break;
			case JSMD_TYPE_UNKNOWN:
			case JSMD_TYPE_INVALID_BSON:
			default:
				J_INFO("Invalid type in Namespace scheme %s for column %s",namespace,cur_key)
				return false;
		}
		insert_index++;
	}

	if(sqlite3_step(insert_stmt) != SQLITE_DONE)
	{
		J_WARNING("%s > %s",sqlite3_expanded_sql(insert_stmt),sqlite3_errmsg(backend_db));
		return FALSE;
	}

	return true;
}

static
gboolean 
backend_insert (gchar const* namespace, gchar const* key, bson_t const* node)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	return _backend_insert(namespace,key,node,FALSE);
}

static
gboolean 
backend_update (gchar const* namespace, gchar const* key, bson_t const* node)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	return _backend_insert(namespace,key,node,TRUE);
}

static
gboolean 
backend_delete (gchar const* namespace, gchar const* key)
{
	g_autofree gchar* delete_querry = create_delete_stmt(namespace,key);
	g_autoptr(sqlite3_stmt) delete_stmt = NULL;
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);

	if(delete_querry != NULL)
	{
	if( sqlite3_prepare_v2(backend_db, delete_querry, -1, &delete_stmt, NULL) != SQLITE_OK)
		{
			J_CRITICAL("Failed to prepare insert statement \"%s\" : %s",delete_querry,sqlite3_errmsg(backend_db));
			return FALSE;
		}
	}

	if(sqlite3_step(delete_stmt) != SQLITE_DONE)
	{
		J_WARNING("%s > %s",sqlite3_expanded_sql(delete_stmt),sqlite3_errmsg(backend_db));
		return FALSE;
	}

	return TRUE;
}

static
gboolean 
backend_get (gchar const* namespace, gchar const* key, bson_t* node)
{	
	g_autoptr(sqlite3_stmt) get_stmt;
	bson_t namespace_scheme = BSON_INITIALIZER;
	bson_iter_t namespace_scheme_iter;
	g_autofree gchar* querry;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	if(!backend_get_scheme(namespace, &namespace_scheme))
	{
		J_WARNING("There is no scheme for this namespace %s",namespace);
		return FALSE;
	}
	
	querry = create_get_stmt(namespace, key);

	if(! sqlite3_prepare_v2(backend_db,querry,-1,&get_stmt,NULL) == SQLITE_OK)
	{
		goto error;
	}

	if( sqlite3_step(get_stmt) == SQLITE_ROW &&
		  bson_iter_init(&namespace_scheme_iter,&namespace_scheme))
	{
		for(int i = 0; i < sqlite3_column_count(get_stmt); i++)
		{
			size_t bytes = 0;
			size_t length = 0;
			uint8_t const* blob;
			gchar const* value;
			gchar const* c_name = sqlite3_column_name(get_stmt,i);
			if(g_strcmp0(c_name,"key") == 0)
			{
				continue;
			}
			if(!bson_iter_find(&namespace_scheme_iter,c_name))
			{
				J_WARNING("scheme of namespace %s does not have a column %s",namespace,c_name);
				return FALSE;
			}

			switch (sqlite3_column_type(get_stmt,i))
			{
				case SQLITE_INTEGER:
					switch (smd_get_type(&namespace_scheme_iter))
					{
						case JSMD_TYPE_INTEGER: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_64:

							bson_append_int64(node,c_name,-1,sqlite3_column_int64(get_stmt,i));
							break;
						case JSMD_TYPE_INTEGER_32: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_32: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_16: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_16: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_8: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_8:
							bson_append_int32(node,c_name,-1,(int32_t) sqlite3_column_int64(get_stmt,i));
							break;
						default:
							g_error("SQLite Result and namespace scheme definition inconsistent.");
							goto error;
					}
					break;
				case SQLITE_FLOAT:
					switch(smd_get_type(&namespace_scheme_iter))
					{
						case JSMD_TYPE_FLOAT: __attribute__ ((fallthrough));
						case JSMD_TYPE_FLOAT_16: __attribute__ ((fallthrough));
						case JSMD_TYPE_FLOAT_32: __attribute__ ((fallthrough));
						case JSMD_TYPE_FLOAT_64:
							bson_append_double(node,c_name,-1,sqlite3_column_double(get_stmt,i));
							break;
						default:
							g_error("SQLite Result and namespace scheme definition inconsistent.");
							goto error;
					}
					break;
				case SQLITE3_TEXT:
					switch(smd_get_type(&namespace_scheme_iter))
					{
						case JSMD_TYPE_DATE_TIME: __attribute__ ((fallthrough));
						case JSMD_TYPE_TEXT:
							value = (const gchar*) sqlite3_column_text(get_stmt,i);
							if(value != NULL)
							{
								bson_append_utf8(node,c_name,-1,value,-1);
							}
							else
							{
								J_WARNING("Namespace %s for key %s on column %s returned NULL",namespace,key,c_name);
							}
							break;
						default:
							g_error("SQLite Result and namespace scheme definition inconsistent.");
							goto error;
					}
					break;
				case SQLITE_BLOB:
					switch(smd_get_type(&namespace_scheme_iter))
					{
						case JSMD_TYPE_FLOAT_256:
							bytes += 16; __attribute__ ((fallthrough)); // Size difference from 256bits to 128bits 
						case JSMD_TYPE_FLOAT_128: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_128: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_128: 
							bytes += 8; __attribute__ ((fallthrough)); // Size difference from 128bits to 64bits 
						case JSMD_TYPE_UNSIGNED_INTEGER: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_64:
							bytes += 8; // Size of 64 Bit unsigned integer
							length = sqlite3_column_bytes(get_stmt,i);
							if(bytes != length)
							{
								g_error("BLOB size mismatch.");
								goto error;
							}

							blob = sqlite3_column_blob(get_stmt,i);
							bson_append_binary(node,c_name,-1,BSON_SUBTYPE_BINARY,blob,length);
							break;
						default:
							g_error("SQLite Result and namespace scheme definition inconsistent.");
							goto error;
					}
					break;
				default:
				  // This should not happen
					g_error("Found not supported column type in sqlite result");
					goto error;
					break;
			}
		}
	}
	else
	{
		// ERROR or NOT_FOUND
		return FALSE;
	}
	

	return TRUE;
	error:
			g_error("A non recoverable Error occured");
			return FALSE;
}

static
gboolean 
backend_search (bson_t* args, gpointer* result_pointer)
{
	g_return_val_if_fail(args != NULL, FALSE);
	g_return_val_if_fail(result_pointer != NULL, FALSE);

	return TRUE;
}

static
gboolean 
backend_search_namespace (bson_t* args, gpointer* result_pointer,gchar const* namespace)
{
	g_return_val_if_fail(args != NULL, FALSE);
	g_return_val_if_fail(result_pointer != NULL, FALSE);
	g_return_val_if_fail(namespace != NULL, FALSE);

	return TRUE;
}

static
gboolean 
backend_iterate (gpointer result_pointer, bson_t* result_item)
{
	g_return_val_if_fail(result_pointer != NULL, FALSE);
	g_return_val_if_fail(result_item != NULL, FALSE);

	return TRUE;
}

static
gboolean 
backend_error (bson_t* error_item)
{
	g_return_val_if_fail(error_item != NULL, FALSE);
	return TRUE;
}

static
JBackend sqlite_backend = {
	.type = J_BACKEND_TYPE_SMD,
	.component = J_BACKEND_COMPONENT_SERVER,
	.smd = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_apply_scheme = backend_apply_scheme,
		.backend_get_scheme = backend_get_scheme,
		.backend_insert = backend_insert,
		.backend_update = backend_update,
		.backend_delete = backend_delete,
		.backend_get = backend_get,
		.backend_search = backend_search,
		.backend_search_namespace = backend_search_namespace,
    .backend_iterate = backend_iterate,
    .backend_error = backend_error
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &sqlite_backend;
}