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

/*
 * Helper Functions
 */
static
int64_t 
smd_get_type(bson_iter_t* iter)
{
	int64_t jsmd_type = JSMD_TYPE_INVALID_BSON;
	if(bson_iter_type(iter) == BSON_TYPE_UTF8)
	{
		g_autofree const gchar* elem = bson_iter_utf8(iter, NULL);
		jsmd_type = j_smd_type_string2type(elem);
	}
	else if (bson_iter_type(iter) == BSON_TYPE_INT64)
	{
		jsmd_type = bson_iter_int64(iter);
	}
	return jsmd_type;
}

static
gchar* 
generate_create_table_stmt(gchar const* namespace, bson_t const* scheme)
{
	bson_iter_t iter;
	GString* create_querry = g_string_new(NULL);
	g_string_append_printf(create_querry,
							"CREATE TABLE `%s` (`key` NOT NULL TEXT PRIMARY KEY,",
							namespace);
	if(! bson_iter_init(&iter, scheme))
	{
		goto error;
	}
	bson_iter_next(&iter);
	while(true)
	{
		int64_t jsmd_type = smd_get_type(&iter);

		switch(jsmd_type)
		{
			
			case JSMD_TYPE_INTEGER_8:
				g_string_append_printf(create_querry,
										"\n %s TINYINT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_INTEGER_16:
				g_string_append_printf(create_querry,
										"\n %s SMALLINT NOT NULL",
										bson_iter_key(&iter));
				break;
			
			case JSMD_TYPE_INTEGER_32:
				g_string_append_printf(create_querry,
											"\n %s INT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_INTEGER:
			case JSMD_TYPE_INTEGER_64:
				g_string_append_printf(create_querry,
										"\n %s BIGINT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_UNSIGNED_INTEGER_8:
			case JSMD_TYPE_UNSIGNED_INTEGER_16:
			case JSMD_TYPE_UNSIGNED_INTEGER_32:
				g_string_append_printf(create_querry,
										"\n %s INTEGER NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_TEXT:
				g_string_append_printf(create_querry,
										"\n %s TEXT NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_FLOAT:
			case JSMD_TYPE_FLOAT_16:
			case JSMD_TYPE_FLOAT_32:
			case JSMD_TYPE_FLOAT_64:
				g_string_append_printf(create_querry,
										"\n %s REAL NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_DATE:
				g_string_append_printf(create_querry,
										"\n %s DATE NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_DATE_TIME:
				g_string_append_printf(create_querry,
										"\n %s DATETIME NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_INTEGER_128:
			case JSMD_TYPE_UNSIGNED_INTEGER:
			case JSMD_TYPE_UNSIGNED_INTEGER_64:
			case JSMD_TYPE_UNSIGNED_INTEGER_128:
			case JSMD_TYPE_FLOAT_128:
			case JSMD_TYPE_FLOAT_256:
				g_string_append_printf(create_querry,
										"\n %s BLOB NOT NULL",
										bson_iter_key(&iter));
				break;
			case JSMD_TYPE_UNKNOWN:

			default:
			// TODO: Better handling if a requested type is not available/implemented
				goto error;
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
	error:
		return NULL;
}

static
gchar* 
generate_insert_table_stmt(gchar const* namespace, bson_t const* node)
{
	bson_iter_t iter_node;
	GString* insert_querry = g_string_new(NULL);
	GString* insert_querry_placeholder = g_string_new("?");
	g_string_append_printf(insert_querry, "INSERT INTO `%s` (`key`",namespace);

	if(!bson_iter_init(&iter_node,node))
	{
		goto error;
	}

	bson_iter_next(&iter_node);
	while(true)
	{
		g_string_append_printf(insert_querry, "%s", bson_iter_key(&iter_node));
		g_string_append(insert_querry,"?");
		if(bson_iter_next(&iter_node))
		{
			g_string_append(insert_querry,",");
			g_string_append(insert_querry_placeholder,",");
			continue;
		}
		else
		{
			g_string_append_printf(insert_querry,") VALUES (%s);",(gchar*) insert_querry_placeholder);
			g_string_free(insert_querry_placeholder,true);
			break;
		}
	}
	
	return g_string_free(insert_querry,false);
	error:
		return NULL;
}

static 
gchar*
create_get_stmt(gchar const* namespace, gchar const* key)
{
	GString* get_querry = g_string_new(NULL);

	g_string_append_printf(get_querry,"SELECT * FROM `%s` WHERE `key` = '%s'", namespace, key);
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

	J_INFO("path - %s", path);

	if (sqlite3_open(path, &backend_db) != SQLITE_OK)
	{
		goto error;
	}

	// Create namespace structure

	if (sqlite3_exec(backend_db, "CREATE TABLE IF NOT EXISTS _julea_structure_ (namespace TEXT NOT NULL, cached_scheme BLOB NOT NULL);", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_CRITICAL("failed to create julea_smd management structure: %s",sqlite3_errmsg(backend_db))
		goto error;
	}

	if (sqlite3_exec(backend_db, "CREATE UNIQUE INDEX IF NOT EXISTS _julea_structure_index_ ON _julea_structure_ (namespace);", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_CRITICAL("failed to create julea_smd management structure: %s",sqlite3_errmsg(backend_db))
		goto error;
	}

	return (backend_db != NULL);

error:
	sqlite3_close(backend_db);

	return FALSE;
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
	gchar* create_querry = NULL;
	sqlite3_stmt* insert_scheme_cache;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(scheme != NULL, FALSE);

	if (sqlite3_exec(backend_db, "BEGIN TRANSACTION;", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_WARNING("cannot begin transaction: %s",sqlite3_errmsg(backend_db))
		goto error;
	}

	// Create SQL Create Table statement from json document
	create_querry = generate_create_table_stmt(namespace,scheme);
	if(create_querry != NULL)
	{
		if(sqlite3_exec(backend_db, create_querry, NULL, NULL, NULL) != SQLITE_OK)
		{
			J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db))
			free(create_querry);
			goto error;
		}
		free(create_querry);
	}
	else
	{
		goto error;
	}

	// Insert into namespace and scheme cache
	if(sqlite3_prepare_v2(backend_db, "INSERT INTO _julea_structure_ (namespace, cached_scheme) VALUES (?, ?);", -1, &insert_scheme_cache, NULL) != SQLITE_OK)
	{
		J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db));
	}
	if(sqlite3_bind_text(insert_scheme_cache, 1, namespace, -1, NULL) != SQLITE_OK)
	{
		J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db));
	}
	if(sqlite3_bind_blob(insert_scheme_cache, 2, bson_get_data(scheme), scheme->len, NULL) != SQLITE_OK)
	{
		J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db));
	}
	if(sqlite3_step(insert_scheme_cache) != SQLITE_OK)
	{
		J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db));
	}
	if(sqlite3_finalize(insert_scheme_cache) != SQLITE_OK)
	{
		J_INFO("failed to execute querry: %s : %s",create_querry,sqlite3_errmsg(backend_db));
	}

	if (sqlite3_exec(backend_db, "COMMIT;", NULL, NULL, NULL) != SQLITE_OK)
	{
		J_WARNING("cannot end transaction: %s",sqlite3_errmsg(backend_db))
		goto error;
	}

	return TRUE;
	error:
		return FALSE;
}

static
gboolean 
backend_get_scheme (gchar const* namespace, bson_t* scheme)
{
	sqlite3_stmt* stmt;
	gint ret;
	gconstpointer result = NULL;
	gsize result_len;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(scheme != NULL, FALSE);

	sqlite3_prepare_v2(backend_db, "SELECT value FROM _julea_structure_ WHERE namespace = ?;", -1, &stmt, NULL);
	sqlite3_bind_text(stmt, 1, namespace, -1, NULL);

	ret = sqlite3_step(stmt);

	if (ret == SQLITE_ROW)
	{
		bson_t tmp[1];
	
		result = sqlite3_column_blob(stmt, 0);
		result_len = sqlite3_column_bytes(stmt, 0);

		// FIXME check whether copies can be avoided
		bson_init_static(tmp, result, result_len);
		bson_copy_to(tmp, scheme);
	}

	sqlite3_finalize(stmt);

	return (result != NULL);
}

static
gboolean 
backend_insert (gchar const* namespace, gchar const* key, bson_t const* node)
{
	sqlite3_stmt* insert_stmt;
	bson_iter_t iter_node;
	bson_iter_t iter_scheme;
	bson_t insert_scheme = BSON_INITIALIZER;
	g_autofree gchar* insert_querry;
	int64_t insert_index;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	if(!backend_get_scheme(namespace, &insert_scheme))
	{
		// Namespace not present.
		goto error;
	}

	insert_querry = generate_insert_table_stmt(namespace,node);
	if(insert_querry != NULL)
	{
		if( sqlite3_prepare_v2(backend_db, insert_querry, -1, &insert_stmt, NULL) != SQLITE_OK)
		{
			free(insert_querry);
			goto error;
		}
		free(insert_querry);
	}
	
	if(!(bson_iter_init(&iter_node,node) && bson_iter_init(&iter_scheme,&insert_scheme)))
	{
		goto error;
	}
	insert_index = 2;
	sqlite3_bind_text(insert_stmt, 1, key,-1,NULL);
	bson_iter_next(&iter_node);
	while(true)
	{
		JSMD_TYPE jsmd_type;
		uint32_t bytes = 0; // Size of binary data;
		const uint8_t* blob;
		const gchar* cur_key = bson_iter_key(&iter_node);
		jsmd_type = smd_get_type(&iter_scheme);
		if(! bson_iter_find(&iter_scheme,cur_key) )
		{
			// Scheme does not have requested column
			goto error;
		}

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
					// Wrong type in node
					goto error;
				}
				sqlite3_bind_int64(insert_stmt, insert_index, bson_iter_int64(&iter_node));
				break;
			case JSMD_TYPE_TEXT:
				if( bson_iter_type(&iter_node) != BSON_TYPE_UTF8)
				{
					// Wrong type in node
					goto error;
				}
				sqlite3_bind_text(insert_stmt, insert_index, bson_iter_utf8(&iter_node,NULL),-1,NULL);
				break;
			case JSMD_TYPE_FLOAT: __attribute__ ((fallthrough));
			case JSMD_TYPE_FLOAT_16: __attribute__ ((fallthrough));
			case JSMD_TYPE_FLOAT_32: __attribute__ ((fallthrough));
			case JSMD_TYPE_FLOAT_64:
				if( bson_iter_type(&iter_node) != BSON_TYPE_DOUBLE)
				{
					// Wrong type in node
					goto error;
				}
				sqlite3_bind_double(insert_stmt, insert_index, bson_iter_double(&iter_node));
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
					// Wrong type in node
					goto error;
				}
				
				bson_iter_binary(&iter_node, NULL, &bytes, &blob);
				sqlite3_bind_blob(insert_stmt, insert_index, &blob , bytes , NULL);
				break;
			case JSMD_TYPE_DATE_TIME: __attribute__ ((fallthrough));
			case JSMD_TYPE_DATE: 
				if( bson_iter_type(&iter_node) != BSON_TYPE_INT64)
				{
					// Wrong type in node
					goto error;
				}
				sqlite3_bind_int64(insert_stmt, insert_index, bson_iter_int64(&iter_node));
				break;
			case JSMD_TYPE_UNKNOWN:
			case JSMD_TYPE_INVALID_BSON:
			default:
			// TODO: Better handling if a requested type is not available/implemented
				goto error;
		}
		if(bson_iter_next(&iter_node))
		{
			insert_index++;
			continue;
		}
		else
		{
			break;
		}	
	}

	bson_destroy(&insert_scheme);
	return true;
	error:
		bson_destroy(&insert_scheme);
		return false;
}

static
gboolean 
backend_update (gchar const* namespace, gchar const* key, bson_t const* node)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	return TRUE;
}

static
gboolean 
backend_delete (gchar const* namespace, gchar const* key, bson_t const* node)
{
	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	return TRUE;
}

static
gboolean 
backend_get (gchar const* namespace, gchar const* key, bson_t* node)
{	
	sqlite3_stmt* get_stmt;
	bson_t namespace_scheme = BSON_INITIALIZER;
	bson_iter_t namespace_scheme_iter;
	g_autofree gchar* querry;

	g_return_val_if_fail(namespace != NULL, FALSE);
	g_return_val_if_fail(key != NULL, FALSE);
	g_return_val_if_fail(node != NULL, FALSE);

	if(!backend_get_scheme(namespace, &namespace_scheme))
	{
		// Namespace not present.
		goto error;
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
			if(g_strcmp0(c_name,"key"))
			{
				continue;
			}
			bson_iter_find(&namespace_scheme_iter,c_name);

			switch (sqlite3_column_type(get_stmt,i))
			{
				case SQLITE_INTEGER:
					switch (smd_get_type(&namespace_scheme_iter))
					{
						case JSMD_TYPE_INTEGER: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_64:
							bson_append_int64(node,c_name,strlen(c_name),sqlite3_column_int64(get_stmt,i));
							break;
						case JSMD_TYPE_INTEGER_32: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_32: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_16: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_16: __attribute__ ((fallthrough));
						case JSMD_TYPE_INTEGER_8: __attribute__ ((fallthrough));
						case JSMD_TYPE_UNSIGNED_INTEGER_8:
							bson_append_int32(node,c_name,strlen(c_name),(int32_t) sqlite3_column_int64(get_stmt,i));
							break;
						case JSMD_TYPE_DATE:
						case JSMD_TYPE_DATE_TIME:
							bson_append_int64(node,c_name,strlen(c_name),(int64_t) sqlite3_column_int64(get_stmt,i));
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
							bson_append_double(node,c_name,strlen(c_name),sqlite3_column_double(get_stmt,i));
							break;
						default:
							g_error("SQLite Result and namespace scheme definition inconsistent.");
							goto error;
					}
					break;
				case SQLITE3_TEXT:
					switch(smd_get_type(&namespace_scheme_iter))
					{
						case JSMD_TYPE_TEXT:
							value = sqlite3_column_blob(get_stmt,i);
							bson_append_utf8(node,c_name,strlen(c_name),value,strlen(value));
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
							bson_append_binary(node,c_name,strlen(c_name),BSON_SUBTYPE_BINARY,blob,length);
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