from behave import *

from steps.utils import hash_value

import os
import sys
import logging
import subprocess
import requests
import time
from base64 import b64encode, b64decode
from hamcrest import assert_that, equal_to, is_not, contains_string
from signal import SIGHUP

class SeedUnitTest:
    def __init__(self, alias, replaces):
        self.alias = alias
        self.replaces = replaces

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
    ensure_dbt_rpc(context)
    context.dbt_rpc.send_signal(SIGHUP)
    wait_dbt_rpc_state(context, 'ready')

def dbt_rcp_request(context, params):
    """
    run a rpc query with params. It will return a request_id
    keep on checking that request until it's not in running state
    """
    ensure_dbt_rpc(context)
    resp = requests.put(
               url=context.dbt_rpc_url,
               json=params)
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

def dbt_compile(context, models=[]):
    if isinstance(models, list):
        models = " ".join(models)
    run_params = {
        "jsonrpc": "2.0",
        "method": "compile",
        "id": f"{context.step_id}_compile",
        "params": {
            "models": f"{models}",
        }
    }
    resp = dbt_rcp_request(context, run_params)

def get_seed_replacements(context):
    refresh_dbt_rpc(context)
    replacements = []
    for seed in context.seeds:
        orig = '{{X}}'.replace('X', seed.replaces)
        repl = '{{ref("Y")}}'.replace('Y', seed.alias)
        replacements.append(f'{orig}|{repl}')
    sql = "\n".join(replacements)
    compiled_sql = dbt_compile_sql(context, sql)
    result = {}
    for line in compiled_sql.splitlines():
        refs = line.split('|')
        result[refs[0]] = refs[1]
    return result

def dbt_compile_sql(context, sql):
    id = hash_value()
    sql_base64 = b64encode(sql.encode('utf-8')).decode('ascii')
    run_params = {
        "jsonrpc": "2.0",
        "method": "compile_sql",
        "id": id,
        "params": {
            "timeout": 60,
            "sql": sql_base64,
            "name": id
        }
    }
    resp = dbt_rcp_request(context, run_params)
    return resp['result']['results'][0]['compiled_sql']

def dbt_run(context, models=[]):
    if isinstance(models, list):
        models = " ".join(models)
    run_params = {
        "jsonrpc": "2.0",
        "method": "run",
        "id": f"{context.step_id}_run",
        "params": {
            "models": f"{models}",
        }
    }
    resp = dbt_rcp_request(context, run_params)

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
        SeedUnitTest(seed_name, f'ref("{alias}")'))

@when('we compile the query')
def step_impl(context):
    sql = dbt_compile_sql(context, context.text)
    replacements = get_seed_replacements(context)
    for orig, seed in replacements.items():
        sql = sql.replace(orig, seed)
    context.compiled_sql = sql

@when('we list existing models')
def step_impl(context):
    context.dbt = dbt(context, 'ls')
    assert True is not False

@when('we run the load for {model}')
def step_impl(context, model):
    seed_alias = [s.alias for s in context.seed.values()]
    seeds_string = ' '.join(seed_alias)
    seed_load = dbt(context, f'seed --select {seeds_string}')
    assert_that(seed_load.returncode, equal_to(0))
    dbt_compile(context, seed_alias + [model])
    context.dbt = dbt_run(context, model)

@then("dbt didn't fail")
def step_impl(context):
    assert context.failed is False

@then("the compiled query is")
def step_impl(context):
    assert_that(
        context.compiled_sql,
        equal_to(context.text))

@then("dbt failed")
def step_impl(context):
    assert_that(context.dbt.returncode, is_not(equal_to(0)))
