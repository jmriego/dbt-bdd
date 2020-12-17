from behave import *

from steps.utils import hash_value, rpc_server

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
from signal import SIGHUP


class SeedUnitTest:
    def __init__(self, context, alias, hash_id):
        self.context = context
        self.alias = alias
        self.hash_id = hash_id
        self.loaded = False
        self.seed_name = f'{hash_id}_{alias}'
        self.seed_path = os.path.join(
                        context.seeds_path,
                        f'{self.seed_name}.csv')


def dbt_cmd(context, command):
    default_flags = [
        '--target', context.target,
        '--profiles-dir', context.profiles_dir]

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

    while True:
        time.sleep(1)
        context.dbt_rpc.poll()
        if context.dbt_rpc.returncode is not None:
            raise RuntimeError('DBT RPC did not start')
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

        if finished:
            return data

#if the rpc server is not running, start it
def ensure_dbt_rpc(context):
    if not hasattr(context, 'dbt_rpc'):
        context.dbt_rpc = rpc_server(
            os.getcwd(),
            cli_vars=json.dumps({s.alias: s.hash_id for s in context.seeds}),
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
        missing_seeds = [s.alias for s in context.seeds if not s.loaded]
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
    return resp['result']['results'][0]['compiled_sql']

def dbt_run(context, models=[]):
    resp = context.dbt_rpc.run(models)
    return resp['result']

@given('{alias} is loaded with this data')
def step_impl(context, alias):
    ref_hash = context.scenario_id
    seed = SeedUnitTest(context, alias, ref_hash)
    context.seeds.append(seed)
    with open(seed.seed_path, 'w') as f:
        f.write(','.join(context.table.headings))
        f.write('\n')
        for row in context.table.rows:
            f.write(','.join(row.cells))
            f.write('\n')


@when('we compile the query')
def step_impl(context):
    refresh_dbt_rpc(context)
    context.compiled_sql = dbt_compile_sql(context, context.text)

@when('we list existing models')
def step_impl(context):
    refresh_dbt_rpc(context)
    context.dbt = dbt(context, 'ls')
    assert True is not False

@when('we run the load for {model}')
def step_impl(context, model):
    refresh_dbt_rpc(context)
    context.dbt = dbt_run(context, model)

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

@then('both are equal')
def step_impl(context):
    actuals = context.tables['calendar'].rows
    expected = context.tables['report'].rows
    assert_that(
        actuals,
        contains_inanyorder(*expected))
