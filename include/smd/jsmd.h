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

#ifndef JULEA_SMD_SMD_H
#define JULEA_SMD_SMD_H

#if !defined(JULEA_SMD_H) && !defined(JULEA_SMD_COMPILATION)
#error "Only <julea-smd.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>
#include <smd/jsmd-type.h>
#include <smd/jsmd-scheme.h>
#include <smd/jsmd-search.h>


G_BEGIN_DECLS

#undef JSMD_REGISTER_TYPE

struct JSMD;
typedef struct JSMD JSMD;

JSMD* j_smd_new(JSMD_Scheme*, const gchar*);
JSMD* j_smd_ref(JSMD*);
void j_smd_unref(JSMD*);

bool j_smd_field_set_int   (JSMD*, const gchar*, gint64);
bool j_smd_field_set_int8  (JSMD*, const gchar*, gint8);
bool j_smd_field_set_int16 (JSMD*, const gchar*, gint16);
bool j_smd_field_set_int32 (JSMD*, const gchar*, gint32);
bool j_smd_field_set_int64 (JSMD*, const gchar*, gint64);
bool j_smd_field_set_int128(JSMD*, const gchar*, gpointer);
bool j_smd_field_set_uint   (JSMD*, const gchar*, guint);
bool j_smd_field_set_uint8  (JSMD*, const gchar*, guint8);
bool j_smd_field_set_uint16 (JSMD*, const gchar*, guint16);
bool j_smd_field_set_uint32 (JSMD*, const gchar*, guint32);
bool j_smd_field_set_uint64 (JSMD*, const gchar*, guint64);
bool j_smd_field_set_uint128(JSMD*, const gchar*, gpointer);
bool j_smd_field_set_float   (JSMD*, const gchar*, gdouble);
bool j_smd_field_set_float16 (JSMD*, const gchar*, gpointer);
bool j_smd_field_set_float32 (JSMD*, const gchar*, gfloat);
bool j_smd_field_set_float64 (JSMD*, const gchar*, gdouble);
bool j_smd_field_set_float128(JSMD*, const gchar*, gpointer);
bool j_smd_field_set_float256(JSMD*, const gchar*, gpointer);
bool j_smd_field_set_text     (JSMD*, const gchar*, const gchar*);
bool j_smd_field_set_date_time(JSMD*, const gchar*, GDateTime*);

gint64   j_smd_field_get_int   (JSMD*, const gchar*, gboolean*);
gint8    j_smd_field_get_int8  (JSMD*, const gchar*, gboolean*);
gint16   j_smd_field_get_int16 (JSMD*, const gchar*, gboolean*);
gint32   j_smd_field_get_int32 (JSMD*, const gchar*, gboolean*);
gint64   j_smd_field_get_int64 (JSMD*, const gchar*, gboolean*);
gpointer j_smd_field_get_int128(JSMD*, const gchar*, gboolean*);
guint64  j_smd_field_get_uint   (JSMD*, const gchar*, gboolean*);
guint8   j_smd_field_get_uint8  (JSMD*, const gchar*, gboolean*);
guint16  j_smd_field_get_uint16 (JSMD*, const gchar*, gboolean*);
guint32  j_smd_field_get_uint32 (JSMD*, const gchar*, gboolean*);
guint64  j_smd_field_get_uint64 (JSMD*, const gchar*, gboolean*);
gpointer j_smd_field_get_uint128(JSMD*, const gchar*, gboolean*);
gdouble  j_smd_field_get_float   (JSMD*, const gchar*, gboolean*);
gpointer j_smd_field_get_float16 (JSMD*, const gchar*, gboolean*);
gfloat   j_smd_field_get_float32 (JSMD*, const gchar*, gboolean*);
gdouble  j_smd_field_get_float64 (JSMD*, const gchar*, gboolean*);
gpointer j_smd_field_get_float128(JSMD*, const gchar*, gboolean*);
gpointer j_smd_field_get_float256(JSMD*, const gchar*, gboolean*);
const gchar*   j_smd_field_get_text     (JSMD*, const gchar*, gboolean*);
GDateTime* j_smd_field_get_date_time(JSMD*, const gchar*, gboolean*);

void j_smd_insert(JSMD*, JBatch*);
void j_smd_get(JSMD*, JBatch*);
void j_smd_update(JSMD*, JBatch*);
void j_smd_delete(JSMD*, JBatch*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JSMD,j_smd_unref)

G_END_DECLS

#endif