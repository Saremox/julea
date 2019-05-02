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

#include <smd/jsmd-type.h>

#include <julea.h>
#include <julea-internal.h>

#define JSMD_REGISTER_TYPE(enum,string) string,
const gchar *JSMD_TYPES_STRING[] = {
  #include <smd/jsmd-type.h>
  NULL
};

#undef JSMD_REGISTER_TYPE

JSMD_TYPE
j_smd_type_string2type(const gchar* string)
{
  JSMD_TYPE ret = 0;
  while(JSMD_TYPES_STRING[ ret ] != NULL)
  {
    if(g_strcmp0(JSMD_TYPES_STRING[ ret ],string) == 0)
      break;
    ret++;
  }
  if(JSMD_TYPES_STRING[ ret ] == NULL)
    ret=JSMD_TYPE_UNKNOWN;
  return ret;
}

gchar* 
j_smd_type_type2string(JSMD_TYPE type_value)
{
  if(type_value > (int64_t) (sizeof(JSMD_TYPES_STRING) / sizeof(JSMD_TYPES_STRING[0])))
    return NULL;
  else
    return g_strdup(JSMD_TYPES_STRING[type_value]);
}

/**
 * @}
 **/