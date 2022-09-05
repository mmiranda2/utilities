import sys
import collections
import subprocess
import time
import random
import datetime
import json
from typing import Dict, List, Tuple
import requests
import concurrent.futures
from functools import wraps
from io import BytesIO
from utilities import helpers, utils


DEFAULT_REQUEST_ENDPOINT = 'https://google.com'
default_request_method = getattr(requests, 'get')
default_request = lambda **kwargs: default_request_method(DEFAULT_REQUEST_ENDPOINT, **kwargs)

DEFAULT_KUBECTL_CMD = 'kubectl get pods'


def str_hash_arg(arg):
    hashed_arg = str(hash(arg))
    if hashed_arg.startswith('-'):
        hashed_arg = hashed_arg[1:]
        return '0' + hashed_arg
    return '1' + hashed_arg


def run_subprocess(cmd, shell=False, env=None, capture_output=True):
    kwargs = dict(shell=shell, env=env, capture_output=capture_output)
    return subprocess.run(cmd, **kwargs)


def run_sh(cmd, shell=True):
    if shell and isinstance(cmd, list):
        cmd = ' '.join(cmd)
    if not shell and isinstance(cmd, str):
        cmd = cmd.split(' ')
    return subprocess.check_output(cmd, shell=shell).encode('utf-8')


def decorator(f, before=None, after=None):
    @wraps(f)
    def wrapper(*args, **kwargs):
        b = before(f, *args, **kwargs) if before else None
        result = f(*args, **kwargs)
        a = after(f, result, b, *args, **kwargs) if after else None
        if a:
            return a
        return result
    return wrapper


def run_decorator(f):
    def before(f, *args, **kwargs):
        print('starting request at', str(datetime.datetime.utcnow()))
        return time.time()
    def after(f, result, b, *args, **kwargs):
        print('end request', result, str(datetime.datetime.utcnow()))
        # return {'runtime': time.time() - b, 'result': result}
        return None

    return decorator(f, before=before, after=after)


class Iterator:
    def __init__(self, iterator, job, *args, **kwargs):
        self.cursor = 0
        self.iterator = iterator
        self.job = job

    def __next__(self, *args, **kwargs):
        ret = next(self.iterator)
        if ret:
            self.cursor += 1
        return ret

    def queued_task(self):
        return (self.job.func, self.job.arguments[self.cursor])


class Job:
    def __init__(self, func, arguments, workers=5, before_callback=None, after_callback=None, wrapper=None):
        self.func = func
        if wrapper:
            self.func = wrapper(self.func)
        if before_callback or after_callback:
            self.func = decorator(self.func, before=before_callback, after=after_callback)
        self.arguments = arguments
        self.iterator = iter((i, self.func, arg) for i, arg in enumerate(self.arguments))
        self.workers = workers
        self.completions = {}
        self.errors = {}
    
    def __len__(self):
        return len(self.arguments)
    
    def __iter__(self):
        # return iter((self.func, arg) for arg in self.arguments)
        return self.iterator
    
    def __getitem__(self, i):
        if i < len(self.arguments):
            return (self.func, self.arguments[i])

    def run(self, runner):
        results = runner.map(self)
        return results

    def get_tasks(self, *indices):
        if not indices:
            indices = range(len(self))
        return ((i, self.func, self.arguments[i]) for i in indices)

    def lock(self):
        self.locked = True
    
    def unlock(self):
        self.locked = False


class Result:
    def __init__(self, index, func, input, output=None, error=None):
        self.index = index
        self.func = func
        self.input = input
        self.output = output
        self.error = error

    def success(self):
        return self.error is None

    def header_record(self):
        return self.func.__name__

    def input_record(self):
        return self.input

    def output_record(self):
        return self.output

    def jsonify(self):
        record = dict(
            header=self.header_record(),
            input=self.input_record(),
            output=self.output_record(),
            index=self.index
        )
        return json.dumps(record).encode('utf-8')
        


class ShellResult:
    def __init__(self, result):
        cmd, env = result.input
        self.cmd, self.env = cmd, env
        self.header = dict(cmd=cmd, env=env)
        self.output = result.output
    
    def output_record(self):
        return utils.stringify_binary_content(self.output)

        
def make_record(result, jsonify=False):
    args, output = result
    cmd, env = args
    header = dict(cmd=cmd, env=env)
    if jsonify:
        output = json.loads(output.decode('utf-8'))
        record = dict(output=output, **header)
        record = json.dumps(record).encode('utf-8')
        return record
    bio = BytesIO()
    bio.write(json.dumps(header).encode('utf-8'))
    bio.write(b'\n')
    bio.write(output)
    record = bio.getvalue()
    return record

class BasePoolRunner:
    _CONCURRENCY = 5
    def __init__(self, executor_class, _concurrency=None): # concurrent.futures.ThreadPoolExecutor
        if _concurrency:
            self._concurrency = _concurrency
        else:
            self._concurrency = BasePoolRunner._CONCURRENCY
        self.executor_class = executor_class
        self.results = {}
        self.queue = {}
        self.completions = []

    def map(self, job):
        return self.flatmap([job])

    def flatmap(self, jobs):
        with self.executor_class(max_workers=self._concurrency) as executor:
            for job in jobs:
                self.submit(executor, job)
                job.lock()
            self.completions = self.as_completed(self.queue)
            self.join()
        for job in jobs:
            job.unlock()
        return self.results
    
    def submit(self, executor, job):
        for i, func, arg in job:
            submission = executor.submit(func, arg)
            self.queue[submission] = (i, func, arg)

    def join(self):
        for future in self.completions:
            self.handle_future(future)

    def handle_future(self, future):
        i, func, arg = self.queue[future]
        result = Result(index=i, func=func, input=arg)
        self.results[i] = result
        try:
            result.output = future.result()
        except Exception as e:
            result.error = e
        return result

    def get(self):
        try:
            future = next(self.completions)
        except:
            result = None
        else:
            result = self.handle_future(future)
        return result

    @staticmethod
    def as_completed(queue):
        return concurrent.futures.as_completed(queue)


class ThreadPoolRunner(BasePoolRunner):
    executor = concurrent.futures.ThreadPoolExecutor
    def __init__(self, num_threads=None):
        super().__init__(self.executor, _concurrency=num_threads)


class ForkPoolRunner(BasePoolRunner):
    executor = concurrent.futures.ProcessPoolExecutor
    def __init__(self, num_processes=None):
        super().__init__(self.executor, _concurrency=num_processes)


class PoolRunner:
    @staticmethod
    def threader(threads=None):
        return ThreadPoolRunner(threads)
    @staticmethod
    def processor(processes=None):
        return ForkPoolRunner(processes)
    @staticmethod
    def pool(pool_size=None, fork=False):
        t = lambda: PoolRunner.threader(pool_size)
        p = lambda: PoolRunner.processor(pool_size)
        return p() if fork else t()


@run_decorator
def http_request(url=None):
    if url is None:
        return default_request()
    return default_request_method(url)


@run_decorator
def execute_binary(args: Tuple[List, Dict]) -> bytes:
    """
    :param args: the command and the env to execute it in. A pair of (binary command: List, environment: Dict)
    :type args: tuple
    :return: stdout bytes
    """
    cmd, env = args
    if len(cmd) > 0:
        completed_proc = run_subprocess(cmd, env=env)
        return completed_proc.stdout

@run_decorator
def kube_get():
    cmd = DEFAULT_KUBECTL_CMD
    return run_subprocess(cmd, shell=True)


def do_function_pool(func, arguments, fork=False):
    """
    :param func: function to concurrently call on each argument
    :type func: function
    :param arguments: list of all the inputs to be concurrently processed
    :type arguments: list
    :param fork: should the pool fork new processes, otherwise threaded
    :type fork: boolean
    :return: stdout bytes
    """
    runner = PoolRunner.pool(fork=fork)
    results = Job(func=func, arguments=arguments).run(runner)
    return results


def do_shell_func_pool(bin_commands, pool_size=10):
    """
    :param bin_commands: the list of executables, each of the form [executable, arg1, arg2, ...]
    :type bin_commands: list[List]
    :return: list of Result objects
    """
    runner = ForkPoolRunner(pool_size)
    return do_function_pool(func=execute_binary, arguments=bin_commands, runner=runner)


def do_http_pool(url=None, num_urls=5, pool_size=5, func=http_request):
    runner = ThreadPoolRunner()
    return do_function_pool(func=func, arguments=[url for i in range(num_urls)], pool_size=pool_size, runner=runner)


def do_kube_pool(pod_names, pool_size=5, func=kube_get):
    runner = ForkPoolRunner(pool_size)
    return do_function_pool(func=func, arguments=pod_names, runner=runner)


def get_random_arguments(filename, num=10):
    count = 0
    with open(filename, 'rt') as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            count += chunk.count('\n')
        chosen = {}
        while len(chosen.keys()) < num and count >= num:
            line_num = random.randint(1, count)
            if line_num not in chosen:
                chosen[line_num] = f.readline(line_num)[:128]
    return (chosen[k] for k in chosen)


def run_parallel(cmd_list, pool_size=10):
    results = do_shell_func_pool(cmd_list, pool_size=pool_size)
    return results


def make_record(result, jsonify=False):
    args, output = result
    cmd, env = args
    header = dict(cmd=cmd, env=env)
    if jsonify:
        output = json.loads(output.decode('utf-8'))
        record = dict(output=output, **header)
        record = json.dumps(record).encode('utf-8')
        return record
    bio = BytesIO()
    bio.write(json.dumps(header).encode('utf-8'))
    bio.write(b'\n')
    bio.write(output)
    record = bio.getvalue()
    return record


def flush_records(filename=None, records=[], jsonify=False):
    mode = 'wb'
    flush = lambda f: list(f.write(record + b'\n') for record in records)
    if filename is None:
        filename = 'results_{}.json'.format(str(time.time()))
    if jsonify:
        mode = 'w'
        flush = lambda f: json.dump(records, f)
    with open(filename, mode) as f:
        flush(f)
    return len(records)


def test_run_parallel(get_command, env={}, size=15, filename=None):
    envs = {'prod': env}
    commands = [get_command(i) for i in range(size)]
    cmd_list = [(cmd, envs['prod']) for cmd in commands]

    results = run_parallel(cmd_list)
    records = [make_record(result) for result in results]

    if filename is None:
        filename = 'test_results_{}.json'.format(str(time.time()))
    num_flushed = flush_records(filename, records)
    
    print('Successfully dumped {} records'.format(num_flushed))


def main():
    get_command = lambda i: ['/bin/bash', '-c', 'echo hello world $ENV {}{}'.format(i, i)]
    test_run_parallel(get_command, env={'ENV': 'dev'})


if __name__ == '__main__':
    main()
