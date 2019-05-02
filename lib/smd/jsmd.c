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
	gchar* namespace;

	/**
	 * The key.
	 **/
	gchar* key;

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

  j_smd_scheme_ref(scheme);

  item = g_slice_new(JSMD);

  item->namespace = g_strdup(j_smd_scheme_namespace(scheme));
  item->key = g_strdup(key);
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

/**
 * @}
 **/
