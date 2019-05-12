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

#ifndef JULEA_SMD_SMD_SEARCH_H
#define JULEA_SMD_SMD_SEARCH_H

#if !defined(JULEA_SMD_H) && !defined(JULEA_SMD_COMPILATION)
#error "Only <julea-smd.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>
#include <smd/jsmd.h>
#include <smd/jsmd-type.h>
#include <smd/jsmd-scheme.h>

G_BEGIN_DECLS

typedef void (*JSMD_Search_foreach_callback) (JSMD*, gpointer);

struct JSMD_Search;
typedef struct JSMD_Search JSMD_Search;

JSMD_Search* j_smd_search_new(JSMD_Scheme*);
JSMD_Search* j_smd_search_ref(JSMD_Search*);
void j_smd_search_unref(JSMD_Search*);

bool j_smd_search_field_set_int   (JSMD_Search*, const gchar*, gint64);
bool j_smd_search_field_set_int8  (JSMD_Search*, const gchar*, gint8);
bool j_smd_search_field_set_int16 (JSMD_Search*, const gchar*, gint16);
bool j_smd_search_field_set_int32 (JSMD_Search*, const gchar*, gint32);
bool j_smd_search_field_set_int64 (JSMD_Search*, const gchar*, gint64);
bool j_smd_search_field_set_int128(JSMD_Search*, const gchar*, gpointer);
bool j_smd_search_field_set_uint   (JSMD_Search*, const gchar*, guint);
bool j_smd_search_field_set_uint8  (JSMD_Search*, const gchar*, guint8);
bool j_smd_search_field_set_uint16 (JSMD_Search*, const gchar*, guint16);
bool j_smd_search_field_set_uint32 (JSMD_Search*, const gchar*, guint32);
bool j_smd_search_field_set_uint64 (JSMD_Search*, const gchar*, guint64);
bool j_smd_search_field_set_uint128(JSMD_Search*, const gchar*, gpointer);
bool j_smd_search_field_set_float   (JSMD_Search*, const gchar*, gdouble);
bool j_smd_search_field_set_float16 (JSMD_Search*, const gchar*, gpointer);
bool j_smd_search_field_set_float32 (JSMD_Search*, const gchar*, gfloat);
bool j_smd_search_field_set_float64 (JSMD_Search*, const gchar*, gdouble);
bool j_smd_search_field_set_float128(JSMD_Search*, const gchar*, gpointer);
bool j_smd_search_field_set_float256(JSMD_Search*, const gchar*, gpointer);
bool j_smd_search_field_set_text     (JSMD_Search*, const gchar*, const gchar*);
bool j_smd_search_field_set_date_time(JSMD_Search*, const gchar*, GDateTime*);

void     j_smd_search_execute(JSMD_Search*, JBatch*);
uint64_t j_smd_search_num_results(JSMD_Search*);
JSMD*    j_smd_search_cur_item(JSMD_Search*);
bson_t*  j_smd_search_cur_item_bson(JSMD_Search*);
bool     j_smd_search_iterate(JSMD_Search*);
bool     j_smd_search_foreach(JSMD_Search*, JSMD_Search_foreach_callback, gpointer);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JSMD_Search,j_smd_search_unref)


G_END_DECLS

#endif