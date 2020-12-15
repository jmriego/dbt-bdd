from behave import *

from steps.utils import hash_value

import os
import re
import sys
import logging
import subprocess
import requests
import time
from base64 import b64encode, b64decode
from hamcrest import assert_that, equal_to, is_not, contains_string
from signal import SIGHUP

class SeedUnitTest:
    def __init__(self, context, alias, original):
        self.alias = alias
        self.original = original
        self.context = context
        self.loaded = False

    @property
    def original_from(self):
        try:
            return self._original_from
        except AttributeError:
            self._original_from = dbt_compile_sql(
                self.context,
                '{{X}}'.replace('X', self.original))
            return self._original_from

    @property
    def replaced_from(self):
        try:
            return self._replaced_from
        except AttributeError:
            self._replaced_from = dbt_compile_sql(
                self.context,
                '{{ref("Y")}}'.replace('Y', self.alias))
            return self._replaced_from

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
        id = hash_value()
        params = {
            "jsonrpc": "2.0",
            "method": "status",
            "id": f"{id}",
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
        port = 8580
        context.dbt_rpc_url = f'http://localhost:{port}/jsonrpc'
        cmd = dbt_cmd(context, f'rpc --port {port}')
        context.dbt_rpc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        wait_dbt_rpc_state(context, 'ready')

# refresh the DBT rpc with the newest seed files
def refresh_dbt_rpc(context):
    if any(not s.loaded for s in context.seeds):
        context.dbt_rpc.send_signal(SIGHUP)
        wait_dbt_rpc_state(context, 'ready')
        missing_seeds = [s.alias for s in context.seeds if not s.loaded]
        compile_run = dbt_compile(context)
        seed_load = dbt_seed(context, missing_seeds)
        # TODO: assert seed load worked
        for s in context.seeds:
            s.loaded = True

    ensure_dbt_rpc(context)

def dbt_rcp_request(context, method, id=None, params={}):
    """
    run a rpc query with params. It will return a request_id
    keep on checking that request until it's not in running state
    """
    ensure_dbt_rpc(context)
    rpc_params = {
        "jsonrpc": "2.0",
        "method": method,
        "id": id if id else hash_value(),
        "params": params
    }
    resp = requests.put(
               url=context.dbt_rpc_url,
               json=rpc_params)
    request_token = resp.json()['result']['request_token']

    poll_params = {
        "jsonrpc": "2.0",
        "method": "poll",
        "id": f"{context.step_id}",
        "params": {
            "request_token": f"{request_token}"
        }
    }
    resp = wait_dbt_rpc_state(context, lambda x: x != 'running', poll_params)
    return resp

def dbt_seed(context, select=[]):
    resp = dbt_rcp_request(
               context,
               "seed",
               f"{context.step_id}_seed",
               {"select": select})
    return resp['result']

def dbt_compile(context, models=[]):
    resp = dbt_rcp_request(
               context,
               "compile",
               f"{context.step_id}_compile",
               {"models": models})
    return resp['result']

def dbt_compile_sql(context, sql):
    sql_base64 = b64encode(sql.encode('utf-8')).decode('ascii')
    name = hash_value(sql_base64)
    resp = dbt_rcp_request(
               context,
               "compile_sql",
               params={"sql": sql_base64,
                       "timeout": 60,
                       "name": name})
    return resp['result']['results'][0]['compiled_sql']

def dbt_run(context, models=[]):
    resp = dbt_rcp_request(
               context,
               "run",
               f"{context.step_id}_compile",
               {"models": models})

@given('{alias} is loaded with this data')
def step_impl(context, alias):
    seed_name = f'{context.scenario_id}_{alias}'
    seed_path = os.path.join(
                    context.seeds_path,
                    f'{seed_name}.csv')

    with open(seed_path, 'w') as f:
        f.write(','.join(context.table.headings))
        f.write('\n')
        for row in context.table.rows:
            f.write(','.join(row.cells))
            f.write('\n')

    context.seeds.append(
        SeedUnitTest(context, seed_name, f'ref("{alias}")'))

@when('we compile the query')
def step_impl(context):
    refresh_dbt_rpc(context)
    sql = dbt_compile_sql(context, context.text)
    for seed in context.seeds:
        orig = seed.original_from
        repl = seed.replaced_from
        # TODO: better replace method that doesnt replace partials
        sql = sql.replace(orig, repl)
    context.compiled_sql = sql

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
