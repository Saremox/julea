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

#ifndef JULEA_SMD_SMD_TYPE_H
#define JULEA_SMD_SMD_TYPE_H

#if !defined(JULEA_SMD_H) && !defined(JULEA_SMD_COMPILATION)
#error "Only <julea-smd.h> can be included directly."
#endif

#define JSMD_REGISTER_TYPE(enum,string) enum,
typedef enum JSMD_TYPE{
#include <smd/jsmd-type.h>
} JSMD_TYPE;

#undef JSMD_REGISTER_TYPE

JSMD_TYPE j_smd_type_string2type(const gchar*);
gchar*    j_smd_type_type2string(JSMD_TYPE);

#endif

#ifdef JSMD_REGISTER_TYPE

JSMD_REGISTER_TYPE(JSMD_TYPE_INVALID_BSON , "ERROR: type of bson value must be of STRING or INTEGER")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNKNOWN,     "ERROR: unknown type in bson value")
JSMD_REGISTER_TYPE(JSMD_TYPE_INTEGER ,    "integer")
JSMD_REGISTER_TYPE(JSMD_TYPE_INTEGER_8  , "integer8")
JSMD_REGISTER_TYPE(JSMD_TYPE_INTEGER_16 , "integer16")
JSMD_REGISTER_TYPE(JSMD_TYPE_INTEGER_32 , "integer32")
JSMD_REGISTER_TYPE(JSMD_TYPE_INTEGER_64 , "integer64")
JSMD_REGISTER_TYPE(JSMD_TYPE_INTEGER_128, "integer128")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNSIGNED_INTEGER ,    "unsigned integer")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNSIGNED_INTEGER_8  , "unsigned integer8")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNSIGNED_INTEGER_16 , "unsigned integer16")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNSIGNED_INTEGER_32 , "unsigned integer32")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNSIGNED_INTEGER_64 , "unsigned integer64")
JSMD_REGISTER_TYPE(JSMD_TYPE_UNSIGNED_INTEGER_128, "unsigned integer128")
JSMD_REGISTER_TYPE(JSMD_TYPE_FLOAT,       "float")
JSMD_REGISTER_TYPE(JSMD_TYPE_FLOAT_16,    "float16")
JSMD_REGISTER_TYPE(JSMD_TYPE_FLOAT_32,    "float32")
JSMD_REGISTER_TYPE(JSMD_TYPE_FLOAT_64,    "float64")
JSMD_REGISTER_TYPE(JSMD_TYPE_FLOAT_128,   "float128")
JSMD_REGISTER_TYPE(JSMD_TYPE_FLOAT_256,   "float256")
JSMD_REGISTER_TYPE(JSMD_TYPE_TEXT ,       "text")
JSMD_REGISTER_TYPE(JSMD_TYPE_DATE_TIME ,       "date time")

#endif