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

G_BEGIN_DECLS

#define JSMD_REGISTER_TYPE(enum,string) enum,
typedef enum JSMD_TYPE{
#include <smd/jsmd-types.h>
} JSMD_TYPE;

#undef JSMD_REGISTER_TYPE

#define JSMD_REGISTER_TYPE(enum,string) string,
const gchar *JSMD_TYPES_STRING[] = {
  #include <smd/jsmd-types.h>
  NULL
};

#undef JSMD_REGISTER_TYPE

struct JSMD;
struct JSMD_Scheme;
struct JSMD_Search;

typedef struct JSMD* JSMD;
typedef struct JSMD_Scheme* JSMD_Scheme;
typedef struct JSMD_Search* JSMD_Search;

typedef void (*JSMD_Search_foreach_callback) (bson_t const*, gpointer);

int64_t j_smd_type_string2int(const gchar*);
gchar*  j_smd_type_int2string(int64_t);

bool j_smd_scheme_new(JSMD_Scheme*);
bool j_smd_scheme_ref(JSMD_Scheme);
bool j_smd_scheme_unref(JSMD_Scheme*);
bool j_smd_scheme_field_add(const gchar*, JSMD_TYPE);
bool j_smd_scheme_field_del(const gchar*);

void j_smd_scheme_apply(const gchar*, const JSMD_Scheme, JBatch*);
void j_smd_scheme_get(const gchar*, JSMD_Scheme*, JBatch*);

bool j_smd_new(const gchar*, const gchar*, JSMD*);
bool j_smd_ref(JSMD);
bool j_smd_unref(JSMD*);
bool j_smd_field_set(JSMD, const gchar*, void*);
bool j_smd_field_get(JSMD, const gchar*, void**);

void j_smd_insert(const gchar*, const gchar*, const bson_t*, JBatch*);
void j_smd_get(const gchar*, const gchar*, const bson_t*, JBatch*);
void j_smd_update(const gchar*, const gchar*, const bson_t*, JBatch*);
void j_smd_delete(const gchar*, const gchar*, const bson_t*, JBatch*);

bool j_smd_search_new(JSMD_Search*,const gchar*);
void j_smd_search_ref(JSMD_Search);
void j_smd_search_unref(JSMD_Search*);

bool j_smd_search_execute(JSMD_Search);
uint64_t j_smd_search_num_results(JSMD_Search);
bool j_smd_search_cur_item(JSMD_Search, bson_t*);
bool j_smd_search_iterate(JSMD_Search);
bool j_smd_search_foreach(JSMD_Search, JSMD_Search_foreach_callback, gpointer);

G_END_DECLS

#endif