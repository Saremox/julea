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

#include <julea-config.h>

#include <glib.h>

#include <string.h>

#include <julea.h>
#include <julea-smd.h>

#include "benchmark.h"

static
void
_benchmark_smd_apply_scheme (BenchmarkResult* result, gboolean use_batch)
{
  guint const n = 50000;

	g_autoptr(JBatch) batch = NULL;
  g_autoptr(JBatch) batch_delete = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

	semantics = j_benchmark_get_semantics();
	batch = j_batch_new(semantics);
  batch_delete = j_batch_new(semantics);

	j_benchmark_timer_start();

  for (guint i = 0; i < n; i++)
	{
		g_autoptr(JSMD_Scheme) scheme = NULL;
		g_autofree gchar* name = NULL;

    name = g_strdup_printf("benchmark-%d", i);
		
		scheme = j_smd_scheme_new(name);

    j_smd_scheme_field_add(scheme,"name",JSMD_TYPE_TEXT);
    j_smd_scheme_field_add(scheme,"loc",JSMD_TYPE_INTEGER);
    j_smd_scheme_field_add(scheme,"coverage",JSMD_TYPE_FLOAT);
    j_smd_scheme_field_add(scheme,"lastrun",JSMD_TYPE_DATE_TIME);

		j_smd_scheme_apply(scheme, batch);
    j_smd_scheme_delete(scheme, batch_delete);

		if (!use_batch)
		{
			j_batch_execute(batch);
		}
	}

  if (use_batch)
	{
		j_batch_execute(batch);
	}

	elapsed = j_benchmark_timer_elapsed();

  j_batch_execute(batch_delete);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
_benchmark_smd_get_scheme (BenchmarkResult* result, gboolean use_batch)
{
  guint const n = 50000;

	g_autoptr(JBatch) batch = NULL;
  g_autoptr(JBatch) batch_create = NULL;
  g_autoptr(JBatch) batch_delete = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

  semantics = j_benchmark_get_semantics();

  batch_create = j_batch_new(semantics);
  batch_delete = j_batch_new(semantics);

  for (guint i = 0; i < n; i++)
	{
		g_autoptr(JSMD_Scheme) scheme = NULL;
		g_autofree gchar* name = NULL;

    name = g_strdup_printf("benchmark-%d", i);
		
		scheme = j_smd_scheme_new(name);

    j_smd_scheme_field_add(scheme,"name",JSMD_TYPE_TEXT);
    j_smd_scheme_field_add(scheme,"loc",JSMD_TYPE_INTEGER);
    j_smd_scheme_field_add(scheme,"coverage",JSMD_TYPE_FLOAT);
    j_smd_scheme_field_add(scheme,"lastrun",JSMD_TYPE_DATE_TIME);

		j_smd_scheme_apply(scheme, batch_create);
    j_smd_scheme_delete(scheme, batch_delete);
	}

  j_batch_execute(batch_create);

	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

  for (guint i = 0; i < n; i++)
	{
		g_autoptr(JSMD_Scheme) scheme = NULL;
		g_autofree gchar* name = NULL;

    name = g_strdup_printf("benchmark-%d", i);
		
		scheme = j_smd_scheme_new(name);

		j_smd_scheme_get(scheme, batch);

		if (!use_batch)
		{
			j_batch_execute(batch);
		}
	}

  if (use_batch)
	{
		j_batch_execute(batch);
	}

	elapsed = j_benchmark_timer_elapsed();

  j_batch_execute(batch_delete);

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
_benchmark_smd_delete_scheme (BenchmarkResult* result, gboolean use_batch)
{
  guint const n = 50000;

	g_autoptr(JBatch) batch = NULL;
  g_autoptr(JBatch) batch_create = NULL;
	g_autoptr(JSemantics) semantics = NULL;
	gdouble elapsed;

  semantics = j_benchmark_get_semantics();

  batch_create = j_batch_new(semantics);

  for (guint i = 0; i < n; i++)
	{
		g_autoptr(JSMD_Scheme) scheme = NULL;
		g_autofree gchar* name = NULL;

    name = g_strdup_printf("benchmark-%d", i);
		
		scheme = j_smd_scheme_new(name);

    j_smd_scheme_field_add(scheme,"name",JSMD_TYPE_TEXT);
    j_smd_scheme_field_add(scheme,"loc",JSMD_TYPE_INTEGER);
    j_smd_scheme_field_add(scheme,"coverage",JSMD_TYPE_FLOAT);
    j_smd_scheme_field_add(scheme,"lastrun",JSMD_TYPE_DATE_TIME);

		j_smd_scheme_apply(scheme, batch_create);
	}

  j_batch_execute(batch_create);

	batch = j_batch_new(semantics);

	j_benchmark_timer_start();

  for (guint i = 0; i < n; i++)
	{
		g_autoptr(JSMD_Scheme) scheme = NULL;
		g_autofree gchar* name = NULL;

    name = g_strdup_printf("benchmark-%d", i);
		
		scheme = j_smd_scheme_new(name);

		j_smd_scheme_delete(scheme, batch);

		if (!use_batch)
		{
			j_batch_execute(batch);
		}
	}

  if (use_batch)
	{
		j_batch_execute(batch);
	}

	elapsed = j_benchmark_timer_elapsed();

	result->elapsed_time = elapsed;
	result->operations = n;
}

static
void
benchmark_smd_apply_scheme(BenchmarkResult* result)
{
  _benchmark_smd_apply_scheme(result,FALSE);
}

static
void
benchmark_smd_apply_scheme_batch(BenchmarkResult* result)
{
  _benchmark_smd_apply_scheme(result,TRUE);
}

static
void
benchmark_smd_get_scheme(BenchmarkResult* result)
{
  _benchmark_smd_get_scheme(result,FALSE);
}

static
void
benchmark_smd_get_scheme_batch(BenchmarkResult* result)
{
  _benchmark_smd_get_scheme(result,TRUE);
}

static
void
benchmark_smd_delete_scheme(BenchmarkResult* result)
{
  _benchmark_smd_delete_scheme(result,FALSE);
}

static
void
benchmark_smd_delete_scheme_batch(BenchmarkResult* result)
{
  _benchmark_smd_delete_scheme(result,TRUE);
}

void
benchmark_smd(void)
{
  j_benchmark_run("/smd/scheme/apply", benchmark_smd_apply_scheme);
  j_benchmark_run("/smd/scheme/apply-batch", benchmark_smd_apply_scheme_batch);
  j_benchmark_run("/smd/scheme/delete", benchmark_smd_delete_scheme);
  j_benchmark_run("/smd/scheme/delete-batch", benchmark_smd_delete_scheme_batch);
  j_benchmark_run("/smd/scheme/get", benchmark_smd_get_scheme);
  j_benchmark_run("/smd/scheme/get-batch", benchmark_smd_get_scheme_batch);
  return;
}