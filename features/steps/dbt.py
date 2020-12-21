from behave import *
from agate import Table

from steps.utils import hash_value, behave2agate, rpc_server

import json
import os
import re
import sys
import logging
import subprocess
import requests
import time
from base64 import b64encode, b64decode
from hamcrest import assert_that, equal_to, is_not, contains_string
from hamcrest.library.collection.issequence_containinginanyorder import contains_inanyorder
from hamcrest.library.collection.issequence_containing import has_items
from signal import SIGHUP


class SeedUnitTest:
    """
    create a seed file for the "alias" model
    hash_id is an id of the combination of seed and step and will allow us to
    identify which data to use for the same table in different features
    """
    def __init__(self, context, alias, hash_id):
        self.context = context
        self.alias = alias
        self.hash_id = hash_id
        self.loaded = False
        self.seed_name = f'{hash_id}_{alias}'
        self.replacement_var = f'ref_{alias}'

    def write_table(self, table):
        self.table = table
        seed_path = os.path.join(
                        self.context.seeds_path,
                        f'{self.seed_name}.csv')
        with open(seed_path, 'w') as f:
            f.write(','.join(table.headings))
            f.write('\n')
            for row in table.rows:
                f.write(','.join(row.cells))
                f.write('\n')


def dbt_cmd(context, command):
    dbt_vars = json.dumps({s.replacement_var: s.hash_id for s in context.seeds})
    default_flags = [
        '--target', context.target,
        '--profiles-dir', context.profiles_dir,
        '--vars', dbt_vars]

    return ['dbt'] + command.split() + default_flags

def dbt(context, command):
    cmd = dbt_cmd(context, command)
    return subprocess.run(cmd, capture_output=True)

def wait_dbt_rpc_state(context, target_state, params=None):
    """
    run the query specified in params until we reach target_state
    if target_state is a function, wait until target_state(curr_state) is true
    by default we check for dbt rpc state but if we receive params we run that query instead
    """

    if params is None:
        params = {
            "jsonrpc": "2.0",
            "method": "status",
            "id": hash_value()
        }

    finished = False
    while not finished:
        time.sleep(1)
        context.dbt_rpc.poll()
        if context.dbt_rpc.returncode is not None:
            raise RuntimeError('DBT RPC not running')
        try:
            resp = requests.put(url=context.dbt_rpc_url, json=params, timeout=5)
            data = resp.json()
        except:
            continue

        state = data['result']['state']
        if hasattr(target_state, '__call__'):
            finished = target_state(state)
        elif isinstance(target_state, str):
            finished = state == target_state

    return data

#if the rpc server is not running, start it
def ensure_dbt_rpc(context):
    if not hasattr(context, 'dbt_rpc'):
        context.dbt_rpc = rpc_server(
            os.getcwd(),
            cli_vars=json.dumps({s.replacement_var: s.hash_id for s in context.seeds}),
            profiles_dir=context.profiles_dir,
            target=context.target)

# refresh the DBT rpc with the newest seed files
def refresh_dbt_rpc(context):
    if any(not s.loaded for s in context.seeds):
        try:
            context.dbt_rpc.exit()
        except AttributeError:
            pass
        ensure_dbt_rpc(context)
        missing_seeds = [s.seed_name for s in context.seeds if not s.loaded]
        seed_load = dbt_seed(context, missing_seeds)
        # TODO: assert seed load worked
        for s in context.seeds:
            s.loaded = True

def dbt_seed(context, select=[]):
    resp = context.dbt_rpc.seed(select)
    return resp['result']

def dbt_compile(context, models=[]):
    resp = context.dbt_rpc.compile(models)
    return resp['result']

def dbt_compile_sql(context, sql):
    resp = context.dbt_rpc.compile_sql(sql)
    result = context.dbt_rpc.async_wait_for_result(resp)
    return result['results'][0]['compiled_sql']

def dbt_run_sql(context, sql):
    resp = context.dbt_rpc.run_sql(sql)
    result = context.dbt_rpc.async_wait_for_result(resp)
    resp_table = result['results'][0]['table']
    column_names = resp_table['column_names']

    # # behave
    # # we have to convert all cells to str for behave tables to work
    # rows = [[str(x) for x in row] for row in resp_table['rows']]
    # table = Table(column_names, rows=rows)
    # return table

    # agate
    rows_dict = [dict(zip(column_names, row)) for row in resp_table['rows']]
    return Table.from_object(rows_dict)


def dbt_run(context, models=[]):
    resp = context.dbt_rpc.run(models)
    result = context.dbt_rpc.async_wait_for_result(resp)
    assert result['state'] == 'success'

@given('{alias} with {data_alias} would have')
def step_impl(context, alias, data_alias):
    seed = SeedUnitTest(context, alias, context.scenario_id)
    context.seed_templates[(alias, data_alias)] = context.table

@given('{alias} is loaded with {data_alias}')
def step_impl(context, alias, data_alias):
    seed = SeedUnitTest(context, alias, context.scenario_id)
    context.seeds.append(seed)
    if data_alias == 'this data':
        table = context.table
    else:
        table = context.seed_templates[(alias, data_alias)]
    seed.write_table(table)

@when('we compile the query')
def step_impl(context):
    refresh_dbt_rpc(context)
    context.compiled_sql = dbt_compile_sql(context, context.text)

@when('we run the query')
def step_impl(context):
    refresh_dbt_rpc(context)
    sql = dbt_compile_sql(context, context.text)
    context.query_result = dbt_run_sql(context, sql)

@when('we query {model}')
def step_impl(context, model):
    refresh_dbt_rpc(context)
    sql = dbt_compile_sql(context, 'select * from {{ref("' + model + '")}}')
    context.query_result = dbt_run_sql(context, sql)

@when('we list existing models')
def step_impl(context):
    refresh_dbt_rpc(context)
    context.dbt = dbt(context, 'ls')
    assert True is not False

@when('we run the load for {model}')
def step_impl(context, model):
    refresh_dbt_rpc(context)
    context.dbt = dbt_run(context, model)
    context.model = model

@then("dbt didn't fail")
def step_impl(context):
    assert context.failed is False

@then("dbt failed")
def step_impl(context):
    assert_that(context.dbt.returncode, is_not(equal_to(0)))

@then("the compiled query is")
def step_impl(context):
    assert_that(
        context.compiled_sql,
        equal_to(context.text))

@then("the compiled query contains")
def step_impl(context):
    assert_that(
        context.compiled_sql,
        contains_string(context.text))

def assert_table_equals(actuals, expected, ignore_other_columns=False):
    if ignore_other_columns:
        assert_that(
            actuals.column_names,
            has_items(*expected.column_names))
        assert_that(
            [r.values() for r in actuals.select(expected.column_names).rows],
            contains_inanyorder(*[r.values() for r in expected.rows]))
    else:
        assert_that(
            actuals.column_names,
            equal_to(expected.column_names))
        assert_that(
            [r.values() for r in actuals.rows],
            contains_inanyorder(*[r.values() for r in expected.rows]))

@then("the results of the query are")
def step_impl(context):
    expected = behave2agate(context.table)
    actuals = context.query_result
    assert_table_equals(actuals, expected)

@then("the results of the query ignoring other columns are")
def step_impl(context):
    expected = behave2agate(context.table)
    actuals = context.query_result
    assert_table_equals(actuals, expected, ignore_other_columns=True)

@then("the results of the model ignoring other columns are")
def step_impl(context):
    model = context.model
    actuals = dbt_run_sql(context, 'select * from {{ref("' + model + '")}}')
    context.query_result = actuals
    expected = behave2agate(context.table)
    assert_table_equals(actuals, expected, ignore_other_columns=True)

@then("the results of the model are")
def step_impl(context):
    model = context.model
    actuals = dbt_run_sql(context, 'select * from {{ref("' + model + '")}}')
    context.query_result = actuals
    expected = behave2agate(context.table)
    assert_table_equals(actuals, expected)
