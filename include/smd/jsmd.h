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
bool j_smd_field_set(JSMD*, const gchar*, void*);
bool j_smd_field_get(JSMD*, const gchar*, void**);

void j_smd_insert(JSMD*, JBatch*);
void j_smd_get(JSMD*, JBatch*);
void j_smd_update(JSMD*, JBatch*);
void j_smd_delete(JSMD*, JBatch*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JSMD,j_smd_unref)

G_END_DECLS

#endif