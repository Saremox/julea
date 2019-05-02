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
#include <smd/jsmd-search.h>

/**
 * \defgroup JSMD SMD SEARCH
 *
 * Data structures and functions for managing Structured Metadata.
 *
 * @{
 **/

struct JSMD_Search
{
  /**
   * Scheme Cache ref 
   */
  JSMD_Scheme* scheme;
  /**
	 * The namespace.
	 **/
	gchar* namespace;
  /**
	 * The reference count.
	 **/
	gint ref_count;
};


JSMD_Search* j_smd_search_new(JSMD_Scheme* scheme)
{
  JSMD_Search* search;

  g_return_val_if_fail(scheme != NULL, NULL);

  j_trace_enter(G_STRFUNC, NULL);

  search = g_slice_new(JSMD_Search);

  search->namespace = g_strdup(j_smd_scheme_namespace(scheme));
  search->scheme = scheme;

  j_smd_scheme_ref(scheme);

  j_trace_leave(G_STRFUNC);

	return search;
}

JSMD_Search* j_smd_search_ref(JSMD_Search* search)
{
	g_return_val_if_fail(search != NULL, NULL);

	j_trace_enter(G_STRFUNC, NULL);

	g_atomic_int_inc(&(search->ref_count));

	j_trace_leave(G_STRFUNC);

	return search;
}

void j_smd_search_unref(JSMD_Search* search)
{
	g_return_if_fail(search != NULL);

	j_trace_enter(G_STRFUNC, NULL);

	if (g_atomic_int_dec_and_test(&(search->ref_count)))
	{
    g_free(search->namespace);
    j_smd_scheme_unref(search->scheme);
		g_slice_free(JSMD_Search, search);
	}

	j_trace_leave(G_STRFUNC);
}

/**
 * @}
 **/