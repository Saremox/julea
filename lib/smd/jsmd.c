/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2017-2018 Michael Straßberger
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

#include <string.h>

#include <bson.h>

#include <smd/jsmd.h>
#include <smd/jsmd-type.h>
#include <smd/jsmd-scheme.h>
#include <smd/jsmd-search.h>

#include <julea.h>
#include <julea-internal.h>

#define UN_CONSTIFY(_t, _v) ((_t)(uintptr_t)(_v)) 


/**
 * \defgroup JSMD SMD
 *
 * Data structures and functions for managing Structured Metadata.
 *
 * @{
 **/

struct JSMD 
{
  /**
	 * The namespace.
	 **/
	const gchar* namespace;

	/**
	 * The key.
	 **/
	const gchar* key;

  /**
   * Scheme Cache ref 
   */
  JSMD_Scheme* scheme;

  /**
   * Bson Tree holding item information 
   */
  bson_t tree;

  /**
	 * The reference count.
	 **/
	gint ref_count;
};

JSMD* j_smd_new(JSMD_Scheme* scheme, const gchar* key)
{
  JSMD* item = NULL;

  g_return_val_if_fail(scheme != NULL, NULL);
  g_return_val_if_fail(key != NULL, NULL);

  j_trace_enter(G_STRFUNC, NULL);

  item = g_slice_new(JSMD);

  item->namespace = g_strdup(j_smd_scheme_namespace(scheme));
  item->key = g_strdup(key);
  item->scheme = j_smd_scheme_ref(scheme);
  item->ref_count = 1;
  bson_init(&item->tree);

  j_trace_leave(G_STRFUNC);

	return item;
}

JSMD* j_smd_ref(JSMD* item)
{
	g_return_val_if_fail(item != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(item->ref_count));

	j_trace_leave(G_STRFUNC);

	return item;
}

void j_smd_unref(JSMD* item)
{
	g_return_if_fail(item != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(item->ref_count)))
	{
    g_free(item->namespace);
    j_smd_scheme_unref(item->scheme);
    bson_destroy(&item->tree);
		g_slice_free(JSMD, item);
	}

	j_trace_leave(G_STRFUNC);
}

static
bool _j_smd_field_get_blob(JSMD* item, const gchar* key, gpointer value, JSMD_TYPE dbtype)
{
  bson_iter_t iter;
  JSMD_TYPE scheme_type;

  g_return_val_if_fail(value != NULL, FALSE);
  g_return_val_if_fail(key != NULL, FALSE);
  g_return_val_if_fail(item != NULL, FALSE);
  g_return_val_if_fail(item->scheme != NULL, FALSE);

  scheme_type = j_smd_scheme_field_get(item->scheme,key);

  g_return_val_if_fail(scheme_type == dbtype,FALSE);

  if(bson_iter_init_find(&iter,&item->tree,key))
  {
    gpointer tmp = NULL;
    switch(scheme_type)
    {
      case JSMD_TYPE_INTEGER:
        *((gint64*) value) = (gint64) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_INTEGER_8:
        *((gint8*) value) = (gint8) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_INTEGER_16:
        *((gint16*) value) = (gint16) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_INTEGER_32:
        *((gint32*) value) = (gint32) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_INTEGER_64:
        *((gint64*) value) = (gint64) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_INTEGER_128:
        bson_iter_binary(&iter,BSON_SUBTYPE_BINARY,NULL,(const uint8_t**) &tmp);
        memcpy(value,tmp,8);
        break;
      case JSMD_TYPE_UNSIGNED_INTEGER:
        break;
      case JSMD_TYPE_UNSIGNED_INTEGER_8:
        *((guint8*) value) = (guint8) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_UNSIGNED_INTEGER_16:
        *((guint16*) value) = (guint16) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_UNSIGNED_INTEGER_32:
        *((guint32*) value) = (guint32) bson_iter_as_int64(&iter);
        break;
      case JSMD_TYPE_UNSIGNED_INTEGER_64:
        bson_iter_binary(&iter,BSON_SUBTYPE_BINARY,NULL,(const uint8_t**) &tmp);
        memcpy(value,tmp,4);
        break;
      case JSMD_TYPE_UNSIGNED_INTEGER_128:
        bson_iter_binary(&iter,BSON_SUBTYPE_BINARY,NULL,(const uint8_t**) &tmp);
        memcpy(value,tmp,8);
        break;
      case JSMD_TYPE_FLOAT:
        *((gdouble*) value) = (gdouble) bson_iter_as_double(&iter);
        break;
      case JSMD_TYPE_FLOAT_16:
        bson_iter_binary(&iter,BSON_SUBTYPE_BINARY,NULL,(const uint8_t**) &tmp);
        memcpy(value,tmp,2);
        break;
      case JSMD_TYPE_FLOAT_32:
        *((gfloat*) value) = (gfloat) bson_iter_as_double(&iter);
        break;
      case JSMD_TYPE_FLOAT_64:
        *((gdouble*) value) = (gdouble) bson_iter_as_double(&iter);
        break;
      case JSMD_TYPE_FLOAT_128:
        bson_iter_binary(&iter,BSON_SUBTYPE_BINARY,NULL,(const uint8_t**) &tmp);
        memcpy(value,tmp,8);
        break;
      case JSMD_TYPE_FLOAT_256:
        bson_iter_binary(&iter,BSON_SUBTYPE_BINARY,NULL,(const uint8_t**) &tmp);
        memcpy(value,tmp,16);
        break;
      case JSMD_TYPE_DATE_TIME: __attribute__ ((fallthrough));
      case JSMD_TYPE_TEXT: __attribute__ ((fallthrough));
      case JSMD_TYPE_INVALID_BSON: __attribute__ ((fallthrough));
      case JSMD_TYPE_UNKNOWN: __attribute__ ((fallthrough));
      default:
        return FALSE;
        break;
    }
  }
  return TRUE;
}

static
bool _j_smd_field_set_blob(JSMD* item, const gchar* key, gpointer value, JSMD_TYPE dbtype)
{
  JSMD_TYPE scheme_type;

  g_return_val_if_fail(value != NULL, FALSE);
  g_return_val_if_fail(key != NULL, FALSE);
  g_return_val_if_fail(item != NULL, FALSE);
  g_return_val_if_fail(item->scheme != NULL, FALSE);

  scheme_type = j_smd_scheme_field_get(item->scheme,key);

  g_return_val_if_fail(scheme_type == dbtype,FALSE);

  if(bson_has_field(&item->tree,key))
  {
    bson_t tmp = BSON_INITIALIZER;
    bson_copy_to_excluding_noinit(&item->tree,&tmp,key,NULL);
    bson_destroy(&item->tree);
    bson_steal(&item->tree,&tmp);
  }

  switch(scheme_type)
  {
    case JSMD_TYPE_INTEGER:
      return bson_append_int64(&(item->tree),key,strlen(key),(gint64) *((gint64*) value));
    case JSMD_TYPE_INTEGER_8:
      return bson_append_int64(&(item->tree),key,strlen(key),(gint64) *((gint8*) value));
		case JSMD_TYPE_INTEGER_16:
      return bson_append_int64(&(item->tree),key,strlen(key),(gint64) *((gint16*) value));
		case JSMD_TYPE_INTEGER_32:
      return bson_append_int64(&(item->tree),key,strlen(key),(gint64) *((gint32*) value));
		case JSMD_TYPE_INTEGER_64:
      return bson_append_int64(&(item->tree),key,strlen(key),(gint64) *((gint64*)value));
    case JSMD_TYPE_INTEGER_128:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,8);
    case JSMD_TYPE_UNSIGNED_INTEGER:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,4);
    case JSMD_TYPE_UNSIGNED_INTEGER_8:
      return bson_append_int64(&(item->tree),key,strlen(key),*((guint8*) value));
		case JSMD_TYPE_UNSIGNED_INTEGER_16:
      return bson_append_int64(&(item->tree),key,strlen(key),*((guint16*) value));
		case JSMD_TYPE_UNSIGNED_INTEGER_32:
      return bson_append_int64(&(item->tree),key,strlen(key),*((guint32*) value));
		case JSMD_TYPE_UNSIGNED_INTEGER_64:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,4);
    case JSMD_TYPE_UNSIGNED_INTEGER_128:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,8);
		case JSMD_TYPE_TEXT:
      return bson_append_utf8(&(item->tree),key,-1,(gchar*) value,-1);
		case JSMD_TYPE_FLOAT:
      return bson_append_double(&(item->tree),key,strlen(key),*((gdouble*) value));
		case JSMD_TYPE_FLOAT_16:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,2);
		case JSMD_TYPE_FLOAT_32:
      return bson_append_double(&(item->tree),key,strlen(key),(gdouble) *((gfloat*) value));
		case JSMD_TYPE_FLOAT_64:
      return bson_append_double(&(item->tree),key,strlen(key),*((gdouble*) value));
    case JSMD_TYPE_FLOAT_128:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,8);
		case JSMD_TYPE_FLOAT_256:
      return bson_append_binary(&(item->tree),key,strlen(key),BSON_SUBTYPE_BINARY,value,16);
		case JSMD_TYPE_DATE_TIME:
      return bson_append_utf8(&(item->tree),key,-1,(gchar*) value,-1);
		case JSMD_TYPE_INVALID_BSON:  __attribute__ ((fallthrough));
		case JSMD_TYPE_UNKNOWN:  __attribute__ ((fallthrough));
		default:
			return FALSE;
			break;
  }
}

bool j_smd_field_set_int   (JSMD* item, const gchar* key, gint64 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_INTEGER);
}

bool j_smd_field_set_int8  (JSMD* item, const gchar* key, gint8 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_INTEGER_8);
}

bool j_smd_field_set_int16 (JSMD* item, const gchar* key, gint16 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_INTEGER_16);
}

bool j_smd_field_set_int32 (JSMD* item, const gchar* key, gint32 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_INTEGER_32);
}

bool j_smd_field_set_int64 (JSMD* item, const gchar* key, gint64 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_INTEGER_64);
}

bool j_smd_field_set_int128(JSMD* item, const gchar* key, void* value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_INTEGER_128);
}

bool j_smd_field_set_uint   (JSMD* item, const gchar* key, guint value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER);
}

bool j_smd_field_set_uint8  (JSMD* item, const gchar* key, guint8 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_8);
}

bool j_smd_field_set_uint16 (JSMD* item, const gchar* key, guint16 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_16);
}

bool j_smd_field_set_uint32 (JSMD* item, const gchar* key, guint32 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_32);
}

bool j_smd_field_set_uint64 (JSMD* item, const gchar* key, guint64 value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_64);
}

bool j_smd_field_set_uint128(JSMD* item, const gchar* key, void* value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_128);
}

bool j_smd_field_set_float   (JSMD* item, const gchar* key, double value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_FLOAT);
}

bool j_smd_field_set_float16 (JSMD* item, const gchar* key, void* value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_FLOAT_16);
}

bool j_smd_field_set_float32 (JSMD* item, const gchar* key, gfloat value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_FLOAT_32);
}

bool j_smd_field_set_float64 (JSMD* item, const gchar* key, gdouble value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_FLOAT_64);
}

bool j_smd_field_set_float128(JSMD* item, const gchar* key, void* value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_FLOAT_128);
}

bool j_smd_field_set_float256(JSMD* item, const gchar* key, void* value)
{
  return _j_smd_field_set_blob(item,key,&value,JSMD_TYPE_FLOAT_256);
}

bool j_smd_field_set_text     (JSMD* item, const gchar* key, const gchar* value)
{
  return _j_smd_field_set_blob(item,key,value,JSMD_TYPE_TEXT);
}

bool j_smd_field_set_date_time(JSMD* item, const gchar* key, GDateTime* value)
{
  g_autofree gchar* tmp = NULL;

  tmp = g_date_time_format(value,"%F %T%:z");
  return _j_smd_field_set_blob(item,key,tmp,JSMD_TYPE_DATE_TIME);
}

gint64   j_smd_field_get_int   (JSMD* item, const gchar* key, gboolean* retval)
{
  gint64 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_INTEGER);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gint8    j_smd_field_get_int8  (JSMD* item, const gchar* key, gboolean* retval)
{
  gint8 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_INTEGER_8);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gint16   j_smd_field_get_int16 (JSMD* item, const gchar* key, gboolean* retval)
{
  gint16 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_INTEGER_16);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gint32   j_smd_field_get_int32 (JSMD* item, const gchar* key, gboolean* retval)
{
  gint32 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_INTEGER_32);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gint64   j_smd_field_get_int64 (JSMD* item, const gchar* key, gboolean* retval)
{
  gint64 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_INTEGER_64);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gpointer j_smd_field_get_int128(JSMD* item, const gchar* key, gboolean* retval)
{
  gpointer value = malloc(8);
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_INTEGER_128);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

guint64  j_smd_field_get_uint   (JSMD* item, const gchar* key, gboolean* retval)
{
  guint64 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

guint8   j_smd_field_get_uint8  (JSMD* item, const gchar* key, gboolean* retval)
{
  guint8 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_8);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

guint16  j_smd_field_get_uint16 (JSMD* item, const gchar* key, gboolean* retval)
{
  guint16 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_16);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

guint32  j_smd_field_get_uint32 (JSMD* item, const gchar* key, gboolean* retval)
{
  guint32 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_32);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

guint64  j_smd_field_get_uint64 (JSMD* item, const gchar* key, gboolean* retval)
{
  guint64 value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_64);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gpointer j_smd_field_get_uint128(JSMD* item, const gchar* key, gboolean* retval)
{
  gpointer value = malloc(8);
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_UNSIGNED_INTEGER_128);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gdouble  j_smd_field_get_float   (JSMD* item, const gchar* key, gboolean* retval)
{
  gdouble value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_FLOAT);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gpointer j_smd_field_get_float16 (JSMD* item, const gchar* key, gboolean* retval)
{
  gpointer value = malloc(2);
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_FLOAT_16);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gfloat   j_smd_field_get_float32 (JSMD* item, const gchar* key, gboolean* retval)
{
  gfloat value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_FLOAT_32);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gdouble  j_smd_field_get_float64 (JSMD* item, const gchar* key, gboolean* retval)
{
  gdouble value;
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_FLOAT_64);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gpointer j_smd_field_get_float128(JSMD* item, const gchar* key, gboolean* retval)
{
  gpointer value = malloc(8);
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_FLOAT_128);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

gpointer j_smd_field_get_float256(JSMD* item, const gchar* key, gboolean* retval)
{
  gpointer value = malloc(16);
  gboolean ret = _j_smd_field_get_blob(item,key,&value,JSMD_TYPE_FLOAT_256);
  if(retval != NULL)
  {
    *retval = ret;
  }
  return value;
}

static gchar* _j_smd_field_get_text(JSMD* item, const gchar* key, gboolean* retval, JSMD_TYPE dbtype)
{
  bson_iter_t iter;
  JSMD_TYPE scheme_type;

  g_return_val_if_fail(key != NULL, NULL);
  g_return_val_if_fail(item != NULL, NULL);
  g_return_val_if_fail(item->scheme != NULL, NULL);

  scheme_type = j_smd_scheme_field_get(item->scheme,key);

  g_return_val_if_fail(scheme_type == dbtype,NULL);

  if(bson_iter_init_find(&iter,&item->tree,key))
  {
    if(retval != NULL)
    {
      *retval = TRUE;
    }
    return g_strdup(bson_iter_utf8(&iter,NULL));
  }
  else
  {
    if(retval != NULL)
    {
      *retval = FALSE;
    }
    return NULL;
  }
}

const gchar* j_smd_field_get_text(JSMD* item, const gchar* key, gboolean* retval)
{
  return _j_smd_field_get_text(item,key,retval,JSMD_TYPE_TEXT);
}

GDateTime* j_smd_field_get_date_time(JSMD* item, const gchar* key, gboolean* retval)
{
  g_autofree gchar* tmp = _j_smd_field_get_text(item,key,retval,JSMD_TYPE_DATE_TIME);
  return g_date_time_new_from_iso8601(tmp,NULL);
}

static
gboolean
_j_smd_insert_update_delete_exec(JList* operations, JSemantics* semantics, JMessageType type)
{
  gboolean ret = TRUE;
	JBackend* smd_backend;
	g_autoptr(JListIterator) it = NULL;
	JMessage** message = NULL;
	JConfiguration* configuration = j_configuration();
	guint32 smd_server_cnt = j_configuration_get_kv_server_count(configuration);

  g_return_val_if_fail(type != J_MESSAGE_SMD_INSERT || 
                       type != J_MESSAGE_SMD_UPDATE ||
                       type != J_MESSAGE_SMD_DELETE, FALSE);
	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	it = j_list_iterator_new(operations);
	smd_backend = j_smd_backend();

	if (smd_backend == NULL)
	{
		message = g_malloc0_n(smd_server_cnt,sizeof(JMessage*));
	}

	j_trace_enter(G_STRFUNC, NULL);

	while (j_list_iterator_next(it))
	{
		JSMD* item = j_list_iterator_get(it);

    const gchar* key = item->key;
    const gchar* namespace = item->namespace;
    gsize namespace_len = strlen(namespace) + 1;
    gsize key_len = strlen(key) + 1;

		if (smd_backend == NULL)
		{
			guint32 server_index = j_helper_hash(namespace) % smd_server_cnt;
			const guint8* bson_blob = NULL;
			gsize bson_size = 0;
      gsize operation_size = namespace_len + key_len;
      if(type != J_MESSAGE_SMD_DELETE)
      {
        bson_blob = bson_get_data(&item->tree);
        bson_size = item->tree.len;
        operation_size += 4 + bson_size;
      }
			
			if(message[server_index] == NULL)
			{
				message[server_index] = j_message_new(type,operation_size);
				j_message_set_safety(message[server_index], semantics);
			}
			j_message_add_operation(message[server_index], operation_size);
			j_message_append_n(message[server_index],namespace,namespace_len);
      j_message_append_n(message[server_index],key,key_len);
      if(type != J_MESSAGE_SMD_DELETE)
			{
        j_message_append_4(message[server_index],&bson_size);
			  j_message_append_n(message[server_index],bson_blob,bson_size);
      }
		}
		else
		{
      if(type == J_MESSAGE_SMD_INSERT)
      {
        if(smd_backend->smd.backend_insert(namespace,key,&item->tree))
        {
          ret = FALSE;
        }
      }
      else if(type == J_MESSAGE_SMD_UPDATE)
      {
        if(smd_backend->smd.backend_update(namespace,key,&item->tree))
        {
          ret = FALSE;
        }
      }
      else if(type == J_MESSAGE_SMD_DELETE)
      {
        if(smd_backend->smd.backend_delete(namespace,key))
        {
          ret = FALSE;
        }
      }
			else
      {
        J_CRITICAL("This should not have happened. %d",type);
        ret = FALSE;
      }
      
		}	
	}

	if (smd_backend == NULL)
	{
		for(guint32 i = 0; i < smd_server_cnt ; i++)
		{
			if(message[i] != NULL)
			{
				GSocketConnection* smd_connection;

				smd_connection = j_connection_pool_pop_smd(i);
				j_message_send(message[i], smd_connection);

				if (j_message_get_flags(message[i]) & J_MESSAGE_FLAGS_SAFETY_NETWORK)
				{
					g_autoptr(JMessage) reply = NULL;

					reply = j_message_new_reply(message[i]);
					j_message_receive(reply, smd_connection);

					/* FIXME do something with reply */
				}

				j_connection_pool_push_smd(i, smd_connection);
				j_message_unref(message[i]);
			}
		}
		g_free(message);
	}
	j_trace_leave(G_STRFUNC);
	return ret;
}

static
gboolean
_j_smd_insert_exec(JList* operations, JSemantics* semantics)
{
  return _j_smd_insert_update_delete_exec(operations,semantics,J_MESSAGE_SMD_INSERT);
}

static
void
_j_smd_insert_free(gpointer data)
{
  g_return_if_fail(data != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	j_smd_unref(data);

	j_trace_leave(G_STRFUNC);
}

static
gboolean
_j_smd_get_exec(JList* operations, JSemantics* semantics)
{
  gboolean ret = TRUE;
	JBackend* smd_backend;
	g_autoptr(JListIterator) it = NULL;
	JMessage** message = NULL;
	JConfiguration* configuration = j_configuration();
	guint32 smd_server_cnt = j_configuration_get_kv_server_count(configuration);

	g_return_val_if_fail(operations != NULL, FALSE);
	g_return_val_if_fail(semantics != NULL, FALSE);

	it = j_list_iterator_new(operations);
	smd_backend = j_smd_backend();

	if (smd_backend == NULL)
	{
		message = g_malloc0_n(smd_server_cnt,sizeof(JMessage*));
	}

	j_trace_enter(G_STRFUNC, NULL);

	while (j_list_iterator_next(it))
	{
    JSMD* item = j_list_iterator_get(it);

    const gchar* key = item->key;
    const gchar* namespace = item->namespace;
    gsize namespace_len = strlen(namespace) + 1;
    gsize key_len = strlen(key) + 1;

		if (smd_backend == NULL)
		{
			guint32 server_index = j_helper_hash(namespace) % smd_server_cnt;
			
			if(message[server_index] == NULL)
			{
				message[server_index] = j_message_new(J_MESSAGE_SMD_GET,namespace_len);
				j_message_set_safety(message[server_index], semantics);
			}

			j_message_add_operation(message[server_index], namespace_len + key_len);
			j_message_append_n(message[server_index],namespace,namespace_len);
      j_message_append_n(message[server_index],key,key_len);
		}
		else
		{
			if(smd_backend->smd.backend_get(namespace,key,&item->tree))
			{
				ret = FALSE;
			}
		}	
	}

	if (smd_backend == NULL)
	{
		JMessage** reply = g_malloc0_n(smd_server_cnt,sizeof(JMessage*));

		for(guint32 i = 0; i < smd_server_cnt ; i++)
		{
			if(message[i] != NULL)
			{
				GSocketConnection* smd_connection;

				smd_connection = j_connection_pool_pop_smd(i);
				j_message_send(message[i], smd_connection);

				if(reply[i] == NULL)
				{
					reply[i] = j_message_new_reply(message[i]);
				}

				j_message_receive(reply[i], smd_connection);
				j_connection_pool_push_smd(i, smd_connection);
			}
		}
		j_list_iterator_free(it);
		it = j_list_iterator_new(operations);
		while (j_list_iterator_next(it))
		{
      JSMD* item = j_list_iterator_get(it);
      const gchar* namespace = item->namespace;

			uint8_t* bson_payload;
			bson_t bson_data = BSON_INITIALIZER;
			gsize bson_payload_length;

			guint32 server_index = j_helper_hash(namespace) % smd_server_cnt;
			
			bson_payload_length = j_message_get_4(reply[server_index]);
			if(bson_payload_length > 0)
			{
				bson_payload = j_message_get_n(reply[server_index],bson_payload_length);
				bson_init_static(&bson_data,bson_payload,bson_payload_length);
				bson_copy_to(&bson_data,&item->tree);
      }
			else
			{
				J_WARNING("Received empty bson tree for namespace %s",namespace);
			}
			
		}
		for(guint32 i = 0; i < smd_server_cnt ; i++)
		{
			j_message_unref(message[i]);
			j_message_unref(reply[i]);
		}
		g_free(message);
		g_free(reply);
	}
	j_trace_leave(G_STRFUNC);
	return ret;
}

static
void
_j_smd_get_free(gpointer data)
{
  g_return_if_fail(data != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	j_smd_unref(data);

	j_trace_leave(G_STRFUNC);
}

static
gboolean
_j_smd_update_exec(JList* operations, JSemantics* semantics)
{
  return _j_smd_insert_update_delete_exec(operations,semantics,J_MESSAGE_SMD_UPDATE);
}

static
void
_j_smd_update_free(gpointer data)
{
  g_return_if_fail(data != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	j_smd_unref(data);

	j_trace_leave(G_STRFUNC);
}

static
gboolean
_j_smd_delete_exec(JList* operations, JSemantics* semantics)
{
  return _j_smd_insert_update_delete_exec(operations,semantics,J_MESSAGE_SMD_DELETE);
}

static
void
_j_smd_delete_free(gpointer data)
{
  g_return_if_fail(data != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	j_smd_unref(data);

	j_trace_leave(G_STRFUNC);
}

void j_smd_insert(JSMD* jsmd, JBatch* batch)
{
	JOperation* op;
  JSMD_Scheme* scheme;
  g_return_if_fail(jsmd != NULL);
  scheme = jsmd->scheme;
	g_return_if_fail(scheme != NULL);
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	op = j_operation_new();
	op->data = j_smd_ref(jsmd);
	op->key = strdup(jsmd->namespace);
	op->exec_func = _j_smd_insert_exec;
	op->free_func = _j_smd_insert_free;

	j_batch_add(batch,op);

	j_trace_leave(G_STRFUNC);
}

void j_smd_get(JSMD* jsmd, JBatch* batch)
{
	JOperation* op;
  JSMD_Scheme* scheme;
  g_return_if_fail(jsmd != NULL);
  scheme = jsmd->scheme;
	g_return_if_fail(scheme != NULL);
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	op = j_operation_new();
	op->data = j_smd_ref(jsmd);
	op->key = strdup(jsmd->namespace);
	op->exec_func = _j_smd_get_exec;
	op->free_func = _j_smd_get_free;

	j_batch_add(batch,op);

	j_trace_leave(G_STRFUNC);
}

void j_smd_update(JSMD* jsmd, JBatch* batch)
{
	JOperation* op;
  JSMD_Scheme* scheme;
  g_return_if_fail(jsmd != NULL);
  scheme = jsmd->scheme;
	g_return_if_fail(scheme != NULL);
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	op = j_operation_new();
	op->data = j_smd_ref(jsmd);
	op->key = strdup(jsmd->namespace);
	op->exec_func = _j_smd_update_exec;
	op->free_func = _j_smd_update_free;

	j_batch_add(batch,op);

	j_trace_leave(G_STRFUNC);
}

void j_smd_delete(JSMD* jsmd, JBatch* batch)
{
	JOperation* op;
  JSMD_Scheme* scheme;
  g_return_if_fail(jsmd != NULL);
  scheme = jsmd->scheme;
	g_return_if_fail(scheme != NULL);
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	op = j_operation_new();
	op->data = j_smd_ref(jsmd);
	op->key = strdup(jsmd->namespace);
	op->exec_func = _j_smd_delete_exec;
	op->free_func = _j_smd_delete_free;

	j_batch_add(batch,op);

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/
