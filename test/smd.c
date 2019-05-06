/*
 * JULEA - Flexible storage framework
 * Copyright (C) 2010-2018 Michael Straßberger
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

#include <julea-config.h>

#include <glib.h>

#include <julea.h>
#include <julea-smd.h>

#include "test.h"

#define TEST_NAMESPACE "__julea_test_smd__"

static
void
test_smd_scheme_apply(void)
{
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);

  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  g_assert(j_smd_scheme_field_add(scheme,"name",JSMD_TYPE_TEXT));
  g_assert(j_smd_scheme_field_add(scheme,"loc",JSMD_TYPE_INTEGER));
  g_assert(j_smd_scheme_field_add(scheme,"coverage",JSMD_TYPE_FLOAT));
  g_assert(j_smd_scheme_field_add(scheme,"lastrun",JSMD_TYPE_DATE_TIME));

  j_smd_scheme_apply(scheme,batch);

  g_assert(j_batch_execute(batch));
}

static
void
test_smd_scheme_get(void)
{
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;

	batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  j_smd_scheme_get(scheme,batch);
  g_assert(j_batch_execute(batch));

  g_assert_cmpint(JSMD_TYPE_TEXT, ==, j_smd_scheme_field_get(scheme,"name"));
  g_assert_cmpint(JSMD_TYPE_INTEGER, ==, j_smd_scheme_field_get(scheme,"loc"));
  g_assert_cmpint(JSMD_TYPE_FLOAT, ==, j_smd_scheme_field_get(scheme,"coverage"));
  g_assert_cmpint(JSMD_TYPE_DATE_TIME, ==, j_smd_scheme_field_get(scheme,"lastrun"));
}


void
test_smd (void)
{
  g_test_add_func("/smd/scheme/apply", test_smd_scheme_apply);
  g_test_add_func("/smd/scheme/get", test_smd_scheme_get);
}