from behave import *

import os
import sys
import logging
import subprocess
import requests
import time
from hamcrest import assert_that, equal_to, is_not, contains_string

def dbt_cmd(cmd, context):
    default_flags = [
        '--target', context.target,
        '--profiles-dir', context.profiles_dir]

    return ['dbt'] + cmd.split() + default_flags

def dbt(command, context):
    cmd = dbt_cmd(command, context)
    return subprocess.run(cmd, capture_output=True)

def wait_dbt_rpc_state(params, context, target_state):
"""
    run the query specified in params until we reach target_state
    if target_state is a function, wait until target_state(curr_state) is true
"""
    while True:
        time.sleep(1)
        try:
            resp = requests.put(url=context.dbt_rpc_url, json=params)
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
        cmd = dbt_cmd(f'rpc --port {port}', context)
        context.dbt_rpc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

    status_params = {
        "jsonrpc": "2.0",
        "method": "status",
        "id": f"ensure_dbt_rpc",
    }
    wait_dbt_rpc_state(status_params, context, 'ready')

def dbt_rcp_request(params, context):
"""
    run a rpc query with params. It will return a request id
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
    resp = wait_dbt_rpc_state(status_params, context, lambda x: x != 'running')
    return resp

def dbt_run_model(model, context):
    run_params = {
        "jsonrpc": "2.0",
        "method": "run",
        "id": f"{context.step_id}",
        "params": {
            "models": f"{model}",
        }
    }
    resp = dbt_rcp_request(run_params, context)

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

    context.seeds[alias] = seed_name

@when('we list existing models')
def step_impl(context):
    context.dbt = dbt('ls', context)
    assert True is not False

@when('we run the load for {model}')
def step_impl(context, model):
    seeds = ' '.join(context.seeds.values())
    seed_load = dbt(f'seed --select {seeds}', context)
    assert_that(seed_load.returncode, equal_to(0))
    context.dbt = dbt_run_model(model, context)

@then("dbt didn't fail")
def step_impl(context):
    assert context.failed is False

@then("dbt failed")
def step_impl(context):
    assert_that(context.dbt.returncode, is_not(equal_to(0)))
