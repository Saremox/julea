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

#include <julea-config.h>

#include <glib.h>
#include <gmodule.h>

#include <julea.h>

static
gboolean 
backend_init (gchar const* path)
{

}

static
void 
backend_fini (void)
{

}

static
gboolean 
backend_apply_scheme (gchar const* namespace, bson_t const* scheme)
{

}

static
gboolean 
backend_get_scheme (gchar const* namespace, bson_t* scheme)
{

}

static
gboolean 
backend_insert (gchar const* namespace, bson_t const* node)
{

}

static
gboolean 
backend_update (gchar const* namespace, bson_t const* node)
{

}

static
gboolean 
backend_delete (gchar const* namespace, bson_t const* node)
{

}

static
gboolean 
backend_search (bson_t* args, gpointer* result_pointer)
{

}

static
gboolean 
backend_iterate (gpointer result_pointer, bson_t* result_item)
{

}

static
gboolean 
backend_error (bson_t* error_item)
{

}

static
JBackend null_backend = {
	.type = J_BACKEND_TYPE_SMD,
	.component = J_BACKEND_COMPONENT_CLIENT | J_BACKEND_COMPONENT_SERVER,
	.smd = {
		.backend_init = backend_init,
		.backend_fini = backend_fini,
		.backend_apply_scheme = backend_apply_scheme,
		.backend_get_scheme = backend_get_scheme,
		.backend_insert = backend_insert,
		.backend_update = backend_update,
		.backend_delete = backend_delete,
		.backend_search = backend_search,
    .backend_iterate = backend_iterate,
    .backend_error = backend_error
	}
};

G_MODULE_EXPORT
JBackend*
backend_info (void)
{
	return &null_backend;
}