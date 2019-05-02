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
};

JSMD_Scheme* j_smd_scheme_new(const gchar* namespace)
{
  JSMD_Scheme* scheme;

  g_return_val_if_fail(namespace != NULL, NULL);

  j_trace_enter(G_STRFUNC, NULL);

  scheme = g_slice_new(JSMD_Scheme);

  scheme->namespace = g_strdup(namespace);

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

const gchar* j_smd_scheme_namespace(JSMD_Scheme* scheme)
{
  g_return_val_if_fail(scheme != NULL, NULL);
  return scheme->namespace;
}

/**
 * @}
 **/