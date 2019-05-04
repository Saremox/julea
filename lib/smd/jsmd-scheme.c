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

#include <julea.h>
#include <julea-internal.h>
#include <smd/jsmd-type.h>
#include <smd/jsmd-scheme.h>

/**
 * \defgroup JSMD SMD SEARCH
 *
 * Data structures and functions for managing Structured Metadata.
 *
 * @{
 **/

struct JSMD_Scheme
{
  /**
	 * The namespace.
	 **/
	gchar* namespace;
  /**
	 * The reference count.
	 **/
	gint ref_count;
	/**
	 * Stored scheme bson definition 
	 */
	bson_t tree;
};

JSMD_Scheme* j_smd_scheme_new(const gchar* namespace)
{
  JSMD_Scheme* scheme;

  g_return_val_if_fail(namespace != NULL, NULL);

  j_trace_enter(G_STRFUNC, NULL);

  scheme = g_slice_new(JSMD_Scheme);

  scheme->namespace = g_strdup(namespace);

	bson_init(&scheme->tree);

  j_trace_leave(G_STRFUNC);

	return scheme;
}

JSMD_Scheme* j_smd_scheme_ref(JSMD_Scheme* scheme)
{
	g_return_val_if_fail(scheme != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(scheme->ref_count));

	j_trace_leave(G_STRFUNC);

	return scheme;
}

void j_smd_scheme_unref(JSMD_Scheme* scheme)
{
	g_return_if_fail(scheme != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(scheme->ref_count)))
	{
    g_free(scheme->namespace);

		g_slice_free(JSMD_Scheme, scheme);
	}

	j_trace_leave(G_STRFUNC);
}

bool j_smd_scheme_field_add(JSMD_Scheme* scheme, const gchar* field_name, JSMD_TYPE type)
{
	bool ret;
	g_return_val_if_fail(scheme != NULL, FALSE);
	g_return_val_if_fail(field_name != NULL, FALSE);
	g_return_val_if_fail(type >= 0, FALSE);

	j_trace_enter(G_STRFUNC, NULL);

	switch (type)
	{
		case JSMD_TYPE_INTEGER_8:
		case JSMD_TYPE_INTEGER_16:
		case JSMD_TYPE_INTEGER_32:
		case JSMD_TYPE_INTEGER:
		case JSMD_TYPE_INTEGER_64:
		case JSMD_TYPE_UNSIGNED_INTEGER_8:
		case JSMD_TYPE_UNSIGNED_INTEGER_16:
		case JSMD_TYPE_UNSIGNED_INTEGER_32:
		case JSMD_TYPE_TEXT:
		case JSMD_TYPE_FLOAT:
		case JSMD_TYPE_FLOAT_16:
		case JSMD_TYPE_FLOAT_32:
		case JSMD_TYPE_FLOAT_64:
		case JSMD_TYPE_DATE:
		case JSMD_TYPE_DATE_TIME:
		case JSMD_TYPE_INTEGER_128:
		case JSMD_TYPE_UNSIGNED_INTEGER:
		case JSMD_TYPE_UNSIGNED_INTEGER_64:
		case JSMD_TYPE_UNSIGNED_INTEGER_128:
		case JSMD_TYPE_FLOAT_128:
		case JSMD_TYPE_FLOAT_256:
			bson_append_int64(&scheme->tree,field_name,strlen(field_name),type);
			ret = TRUE;
			break;
		case JSMD_TYPE_INVALID_BSON:
		case JSMD_TYPE_UNKNOWN:
		default:
			ret = FALSE;
			break;
	}

	j_trace_leave(G_STRFUNC);

	return ret;
}

static gboolean _j_smd_scheme_apply_exec(JList* operations, JSemantics* semantics)
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
		JSMD_Scheme* scheme = j_list_iterator_get(it);
		
		if (smd_backend == NULL)
		{
			gchar* namespace = scheme->namespace;
			gsize namespace_len = strlen(namespace) + 1;
			guint32 server_index = j_helper_hash(namespace) % smd_server_cnt;
			const guint8* bson_blob = bson_get_data(&scheme->tree);
			size_t bson_size = scheme->tree.len;
			
			if(message[server_index] == NULL)
			{
				message[server_index] = j_message_new(J_MESSAGE_SMD_SCHEMA_APPLY,0);
				j_message_set_safety(message[server_index], semantics);
			}
			j_message_add_operation(message[server_index], bson_size + 4 + namespace_len);
			j_message_append_n(message[server_index],namespace,namespace_len);
			j_message_append_4(message[server_index],&bson_size);
			j_message_append_n(message[server_index],bson_blob,bson_size);
		}
		else
		{
			if(smd_backend->smd.backend_apply_scheme(scheme->namespace,&scheme->tree))
			{
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

static void _j_smd_scheme_apply_free(gpointer data)
{
	g_return_if_fail(data != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	j_smd_scheme_unref(data);

	j_trace_leave(G_STRFUNC);
}

static gboolean _j_smd_scheme_get_exec(JList* operations, JSemantics* semantics)
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
		JSMD_Scheme* scheme = j_list_iterator_get(it);

		if (smd_backend == NULL)
		{
			guint32 server_index = j_helper_hash(scheme->namespace) % smd_server_cnt;
			
			if(message[server_index] == NULL)
			{
				message[server_index] = j_message_new(J_MESSAGE_SMD_SCHEMA_APPLY,strlen(scheme->namespace) +1);
				j_message_set_safety(message[server_index], semantics);
			}

			j_message_add_operation(message[server_index], strlen(scheme->namespace) +1);
			j_message_append_n(message[server_index],scheme->namespace,strlen(scheme->namespace) +1);
		}
		else
		{
			if(smd_backend->smd.backend_get_scheme(scheme->namespace,&scheme->tree))
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
				j_message_unref(message[i]);
				j_connection_pool_push_smd(i, smd_connection);
			}
		}
		j_list_iterator_free(it);
		it = j_list_iterator_new(operations);
		while (j_list_iterator_next(it))
		{
			uint8_t* bson_payload;
			bson_t bson_data;
			gsize bson_payload_length;
			JSMD_Scheme* scheme = j_list_iterator_get(it);
			guint32 server_index = j_helper_hash(scheme->namespace) % smd_server_cnt;
			
			bson_payload_length = j_message_get_4(reply[server_index]);
			bson_payload = j_message_get_n(reply[server_index],bson_payload_length);
			bson_init_static(&bson_data,bson_payload,bson_payload_length);
			bson_copy_to(&bson_data,&scheme->tree);
		}
		for(guint32 i = 0; i < smd_server_cnt ; i++)
		{
			j_message_unref(reply[i]);
		}
		g_free(message);
		g_free(reply);
	}
	j_trace_leave(G_STRFUNC);
	return ret;
}

static void _j_smd_scheme_get_free(gpointer data)
{
	g_return_if_fail(data != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	j_smd_scheme_unref(data);

	j_trace_leave(G_STRFUNC);
}

void j_smd_scheme_apply(JSMD_Scheme* scheme, JBatch* batch)
{
	JOperation* op;
	g_return_if_fail(scheme != NULL);
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	op = j_operation_new();
	op->data = j_smd_scheme_ref(scheme);
	op->key = scheme->namespace;
	op->exec_func = _j_smd_scheme_apply_exec;
	op->free_func = _j_smd_scheme_apply_free;

	j_batch_add(batch,op);

	j_trace_leave(G_STRFUNC);
}

void j_smd_scheme_get(JSMD_Scheme* scheme, JBatch* batch)
{
	JOperation* op;
	g_return_if_fail(scheme != NULL);
	g_return_if_fail(batch != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	op = j_operation_new();
	op->data = j_smd_scheme_ref(scheme);
	op->key = scheme->namespace;
	op->exec_func = _j_smd_scheme_get_exec;
	op->free_func = _j_smd_scheme_get_free;

	j_trace_leave(G_STRFUNC);
}

const gchar* j_smd_scheme_namespace(JSMD_Scheme* scheme)
{
  g_return_val_if_fail(scheme != NULL, NULL);
  return scheme->namespace;
}

/**
 * @}
 **/