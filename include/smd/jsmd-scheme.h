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

#ifndef JULEA_SMD_SMD_SCHEME_H
#define JULEA_SMD_SMD_SCHEME_H

#if !defined(JULEA_SMD_H) && !defined(JULEA_SMD_COMPILATION)
#error "Only <julea-smd.h> can be included directly."
#endif

#include <glib.h>

#include <julea.h>
#include <smd/jsmd-type.h>

G_BEGIN_DECLS

struct JSMD_Scheme;
typedef struct JSMD_Scheme JSMD_Scheme;

JSMD_Scheme* j_smd_scheme_new(const gchar*);
JSMD_Scheme* j_smd_scheme_ref(JSMD_Scheme*);
void j_smd_scheme_unref(JSMD_Scheme*);
bool j_smd_scheme_field_add(JSMD_Scheme*, const gchar*, JSMD_TYPE);
JSMD_TYPE j_smd_scheme_field_get(JSMD_Scheme*, const gchar*);

void j_smd_scheme_apply(JSMD_Scheme*, JBatch*);
void j_smd_scheme_get(JSMD_Scheme*, JBatch*);

const gchar* j_smd_scheme_namespace(JSMD_Scheme*);

G_DEFINE_AUTOPTR_CLEANUP_FUNC(JSMD_Scheme,j_smd_scheme_unref)

G_END_DECLS

#endif