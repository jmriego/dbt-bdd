from behave import *
from agate import Table

import hashlib
import os




import subprocess
import base64
import json
import os
import random
import signal
import socket
import time
from contextlib import contextmanager
from typing import Dict, Any, Optional, Union, List

import requests
import yaml

import dbt.flags
from dbt.adapters.factory import get_adapter, register_adapter
from dbt.logger import log_manager
from dbt.main import handle_and_check
from dbt.config import RuntimeConfig




def hash_value(x=None):
    if x is None:
        x = os.urandom(32)
    try:
        return hashlib.md5(x.encode('utf-8')).hexdigest()[:8]
    except AttributeError:
        return hashlib.md5(x).hexdigest()[:8]


def behave2agate(table):
    column_names = table.headings
    rows = table.rows
    rows_dict = [dict(zip(column_names, row)) for row in rows]
    return Table.from_object(rows_dict)


class NoServerException(Exception):
    pass


class ServerProcess():
    def __init__(
        self,
        cwd,
        port,
        profiles_dir,
        cli_vars=None,
        criteria=('ready',),
        target=None,
    ):
        self.cwd = cwd
        self.port = port
        self.criteria = criteria
        self.error = None
        self.cmd = [
            'dbt', 'rpc', '--log-cache-events',
            '--port', str(self.port),
            '--profiles-dir', profiles_dir
        ]
        if cli_vars:
            self.cmd.extend(['--vars', cli_vars])
        if target is not None:
            self.cmd.extend(['--target', target])

    def run(self):
        self.proc = subprocess.Popen(self.cmd, cwd=self.cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return self.proc

    def exit(self):
        self.proc.terminate()
        self.proc.wait()

    def can_connect(self):
        sock = socket.socket()
        try:
            sock.connect(('localhost', self.port))
        except socket.error:
            return False
        sock.close()
        return True

    def _compare_result(self, result):
        return result['result']['state'] in self.criteria

    def status_ok(self):
        result = self.query(
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        return self._compare_result(result)

    def is_up(self):
        if not self.can_connect():
            return False
        return self.status_ok()

    def start(self):
        self.run()
        for _ in range(30):
            if self.is_up():
                break
            time.sleep(0.5)
        if not self.can_connect():
            raise NoServerException('server never appeared!')
        status_result = self.query(
            {'method': 'status', 'id': 1, 'jsonrpc': '2.0'}
        ).json()
        if not self._compare_result(status_result):
            raise NoServerException(
                'Got invalid status result: {}'.format(status_result)
            )

    @property
    def url(self):
        return 'http://localhost:{}/jsonrpc'.format(self.port)

    def query(self, query):
        headers = {'content-type': 'application/json'}
        return requests.post(self.url, headers=headers, data=json.dumps(query))


class Querier:
    def __init__(self, server: ServerProcess):
        self.server = server

    def sighup(self):
        os.kill(self.server.pid, signal.SIGHUP)

    def exit(self):
        self.server.exit()

    def build_request_data(self, method, params, request_id):
        return {
            'jsonrpc': '2.0',
            'method': method,
            'params': params,
            'id': request_id,
        }

    def request(self, method, params=None, request_id=1):
        if params is None:
            params = {}

        data = self.build_request_data(
            method=method, params=params, request_id=request_id
        )
        response = self.server.query(data)
        assert response.ok, f'invalid response from server: {response.text}'
        return response.json()

    def status(self, request_id: int = 1):
        return self.request(method='status', request_id=request_id)

    def wait_for_status(self, expected, times=30) -> bool:
        for _ in range(times):
            time.sleep(0.5)
            status = self.is_result(self.status())
            if status['state'] == expected:
                return True
        return False

    def ps(self, active=True, completed=False, request_id=1):
        params = {}
        if active is not None:
            params['active'] = active
        if completed is not None:
            params['completed'] = completed

        return self.request(method='ps', params=params, request_id=request_id)

    def kill(self, task_id: str, request_id: int = 1):
        params = {'task_id': task_id}
        return self.request(
            method='kill', params=params, request_id=request_id
        )

    def poll(
        self,
        request_token: str,
        logs: Optional[bool] = None,
        logs_start: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {
            'request_token': request_token,
        }
        if logs is not None:
            params['logs'] = logs
        if logs_start is not None:
            params['logs_start'] = logs_start
        return self.request(
            method='poll', params=params, request_id=request_id
        )

    def gc(
        self,
        task_ids: Optional[List[str]] = None,
        before: Optional[str] = None,
        settings: Optional[Dict[str, Any]] = None,
        request_id: int = 1,
    ):
        params = {}
        if task_ids is not None:
            params['task_ids'] = task_ids
        if before is not None:
            params['before'] = before
        if settings is not None:
            params['settings'] = settings
        return self.request(
            method='gc', params=params, request_id=request_id
        )

    def cli_args(self, cli: str, request_id: int = 1):
        return self.request(
            method='cli_args', params={'cli': cli}, request_id=request_id
        )

    def deps(self, request_id: int = 1):
        return self.request(method='deps', request_id=request_id)

    def compile(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='compile', params=params, request_id=request_id
        )

    def run(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='run', params=params, request_id=request_id
        )

    def run_operation(
        self,
        macro: str,
        args: Optional[Dict[str, Any]],
        request_id: int = 1,
    ):
        params = {'macro': macro}
        if args is not None:
            params['args'] = args
        return self.request(
            method='run-operation', params=params, request_id=request_id
        )

    def seed(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        show: bool = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if show is not None:
            params['show'] = show
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='seed', params=params, request_id=request_id
        )

    def snapshot(
        self,
        select: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if exclude is not None:
            params['exclude'] = exclude
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='snapshot', params=params, request_id=request_id
        )

    def snapshot_freshness(
        self,
        select: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        request_id: int = 1,
    ):
        params = {}
        if select is not None:
            params['select'] = select
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='snapshot-freshness', params=params, request_id=request_id
        )

    def test(
        self,
        models: Optional[Union[str, List[str]]] = None,
        exclude: Optional[Union[str, List[str]]] = None,
        threads: Optional[int] = None,
        data: bool = None,
        schema: bool = None,
        request_id: int = 1,
    ):
        params = {}
        if models is not None:
            params['models'] = models
        if exclude is not None:
            params['exclude'] = exclude
        if data is not None:
            params['data'] = data
        if schema is not None:
            params['schema'] = schema
        if threads is not None:
            params['threads'] = threads
        return self.request(
            method='test', params=params, request_id=request_id
        )

    def docs_generate(self, compile: bool = None, request_id: int = 1):
        params = {}
        if compile is not None:
            params['compile'] = True
        return self.request(
            method='docs.generate', params=params, request_id=request_id
        )

    def compile_sql(
        self,
        sql: str,
        name: str = 'test_compile',
        macros: Optional[str] = None,
        request_id: int = 1,
    ):
        sql = base64.b64encode(sql.encode('utf-8')).decode('utf-8')
        params = {
            'name': name,
            'sql': sql,
            'macros': macros,
        }
        return self.request(
            method='compile_sql', params=params, request_id=request_id
        )

    def run_sql(
        self,
        sql: str,
        name: str = 'test_run',
        macros: Optional[str] = None,
        request_id: int = 1,
    ):
        sql = base64.b64encode(sql.encode('utf-8')).decode('utf-8')
        params = {
            'name': name,
            'sql': sql,
            'macros': macros,
        }
        return self.request(
            method='run_sql', params=params, request_id=request_id
        )

    def get_manifest(self, request_id=1):
        return self.request(
            method='get-manifest', params={}, request_id=request_id
        )

    def is_result(self, data: Dict[str, Any], id=None) -> Dict[str, Any]:
        if id is not None:
            assert data['id'] == id
        assert data['jsonrpc'] == '2.0'
        assert 'result' in data
        assert 'error' not in data
        return data['result']

    def is_async_result(self, data: Dict[str, Any], id=None) -> str:
        result = self.is_result(data, id)
        assert 'request_token' in result
        return result['request_token']

    def is_error(self, data: Dict[str, Any], id=None) -> Dict[str, Any]:
        if id is not None:
            assert data['id'] == id
        assert data['jsonrpc'] == '2.0'
        assert 'result' not in data
        assert 'error' in data
        return data['error']

    def async_wait(
        self, token: str, timeout: int = 60, state='success'
    ) -> Dict[str, Any]:
        start = time.time()
        while True:
            time.sleep(0.5)
            response = self.poll(token)
            if 'error' in response:
                return response
            result = self.is_result(response)
            assert 'state' in result
            if result['state'] == state:
                return response
            delta = (time.time() - start)
            assert timeout > delta, \
                f'At time {delta}, never saw {state}.\nLast response: {result}'

    def async_wait_for_result(self, data: Dict[str, Any], state='success'):
        token = self.is_async_result(data)
        return self.is_result(self.async_wait(token, state=state))

    def async_wait_for_error(self, data: Dict[str, Any], state='success'):
        token = self.is_async_result(data)
        return self.is_error(self.async_wait(token, state=state))


def _first_server(cwd, cli_vars, profiles_dir, criteria, target):
    stored = None
    for _ in range(5):
        port = random.randint(20000, 65535)

        proc = ServerProcess(
            cwd,
            cli_vars=cli_vars,
            profiles_dir=str(profiles_dir),
            port=port,
            criteria=criteria,
            target=target,
        )
        try:
            proc.start()
        except NoServerException as exc:
            stored = exc
        else:
            return proc
    if stored:
        raise stored


def rpc_server(
    project_dir, cli_vars, profiles_dir, criteria='ready', target=None
):
    if isinstance(criteria, str):
        criteria = (criteria,)
    else:
        criteria = tuple(criteria)
    proc = _first_server(project_dir, cli_vars, profiles_dir, criteria, target)
    return Querier(proc)
