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
#define TEST_KEY "__ohh_romio__"
#define TEST_DATE "2000-01-01 21:42:42+02:00"

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

static
void
test_smd_new(void)
{
  g_autoptr(JSMD) jsmd = NULL;
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;
  g_autoptr(GDateTime) date = g_date_time_new_from_iso8601(TEST_DATE,NULL);

  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  j_smd_scheme_get(scheme,batch);
  g_assert(j_batch_execute(batch));

  jsmd = j_smd_new(scheme,TEST_KEY);
  g_assert(jsmd != NULL);
}

static
void
test_smd_field_set_get(void)
{
  g_autoptr(JSMD) jsmd = NULL;
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;
  g_autoptr(GDateTime) lastrun = NULL;
  g_autoptr(GDateTime) date = g_date_time_new_from_iso8601(TEST_DATE,NULL);

  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  j_smd_scheme_get(scheme,batch);
  g_assert(j_batch_execute(batch));

  jsmd = j_smd_new(scheme,TEST_KEY);
  g_assert(j_smd_field_set_text(jsmd,"name","Romeo"));
  g_assert(j_smd_field_set_int(jsmd,"loc",4242));
  g_assert(j_smd_field_set_float(jsmd,"coverage",3.14159));
  g_assert(j_smd_field_set_date_time(jsmd,"lastrun",date));

  g_assert_cmpstr(j_smd_field_get_text(jsmd,"name",NULL),==,"Romeo");
  g_assert_cmpint(j_smd_field_get_int(jsmd,"loc",NULL),==,4242);
  g_assert_cmpfloat(j_smd_field_get_float(jsmd,"coverage",NULL)- 3.14159,<,0.2);
  lastrun = j_smd_field_get_date_time(jsmd,"lastrun",NULL);

  g_assert_cmpstr(g_date_time_format(lastrun,"%F %T%:z"),==,g_date_time_format(date,"%F %T%:z"));
}

static
void
test_smd_insert(void)
{
  g_autoptr(JSMD) jsmd = NULL;
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;
  g_autoptr(GDateTime) date = g_date_time_new_from_iso8601(TEST_DATE,NULL);

  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  j_smd_scheme_get(scheme,batch);
  g_assert(j_batch_execute(batch));

  j_batch_unref(batch);
  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);

  jsmd = j_smd_new(scheme,TEST_KEY);
  g_assert(j_smd_field_set_text(jsmd,"name","Romeo"));
  g_assert(j_smd_field_set_int(jsmd,"loc",4242));
  g_assert(j_smd_field_set_float(jsmd,"coverage",3.14159));
  g_assert(j_smd_field_set_date_time(jsmd,"lastrun",date));

  j_smd_insert(jsmd,batch);
  g_assert(j_batch_execute(batch));
}

static
void
test_smd_get(void)
{
  g_autoptr(JSMD) jsmd = NULL;
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;
  g_autoptr(GDateTime) lastrun = NULL;
  g_autoptr(GDateTime) date = g_date_time_new_from_iso8601(TEST_DATE,NULL);

  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  j_smd_scheme_get(scheme,batch);
  g_assert(j_batch_execute(batch));

  j_batch_unref(batch);
  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);

  jsmd = j_smd_new(scheme,TEST_KEY);

  j_smd_get(jsmd,batch);
  g_assert(j_batch_execute(batch));

  g_assert_cmpstr(j_smd_field_get_text(jsmd,"name",NULL),==,"Romeo");
  g_assert_cmpint(j_smd_field_get_int(jsmd,"loc",NULL),==,4242);
  g_assert_cmpfloat(j_smd_field_get_float(jsmd,"coverage",NULL)- 3.14159,<,0.2);
  lastrun = j_smd_field_get_date_time(jsmd,"lastrun",NULL);

  g_assert_cmpstr(g_date_time_format(lastrun,"%F %T%:z"),==,g_date_time_format(date,"%F %T%:z"));
}

static
void
test_smd_update(void)
{
  g_autoptr(JSMD) jsmd = NULL;
  g_autoptr(JSMD_Scheme) scheme = NULL;
  g_autoptr(JBatch) batch = NULL;

  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  scheme = j_smd_scheme_new(TEST_NAMESPACE);
  g_assert(scheme != NULL);

  j_smd_scheme_get(scheme,batch);
  g_assert(j_batch_execute(batch));

  j_batch_unref(batch);
  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);

  jsmd = j_smd_new(scheme,TEST_KEY);

  j_smd_get(jsmd,batch);
  g_assert(j_batch_execute(batch));

  g_assert(j_smd_field_set_text(jsmd,"name","Julea"));

  j_batch_unref(batch);
  batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
  g_assert(batch != NULL);
  j_smd_update(jsmd,batch);
  g_assert(j_batch_execute(batch));
  
  {
    g_autoptr(JSMD) jsmd_check = NULL;

    jsmd_check = j_smd_new(scheme,TEST_KEY);

    j_batch_unref(batch);
    batch = j_batch_new_for_template(J_SEMANTICS_TEMPLATE_DEFAULT);
    g_assert(batch != NULL);
    
    j_smd_get(jsmd_check,batch);
    g_assert(j_batch_execute(batch));

    g_assert_cmpstr(j_smd_field_get_text(jsmd,"name",NULL),==,"Julea");
  }
}

void
test_smd (void)
{
  g_test_add_func("/smd/scheme/apply", test_smd_scheme_apply);
  g_test_add_func("/smd/scheme/get", test_smd_scheme_get);
  g_test_add_func("/smd/item/new", test_smd_new);
  g_test_add_func("/smd/item/field_get_set", test_smd_field_set_get);
  g_test_add_func("/smd/item/insert", test_smd_insert);
  g_test_add_func("/smd/item/get", test_smd_get);
  g_test_add_func("/smd/item/update", test_smd_update);
}