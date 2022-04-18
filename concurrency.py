import sys
import collections
import subprocess
import time
import random
import datetime
import json
import requests
import concurrent.futures
from functools import wraps
from io import BytesIO


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


class ArgIterator:
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
        self.iterator = ArgIterator(iter(self.get_tasks()), job=self)
        self.workers = workers
    
    def __len__(self):
        return len(self.arguments)
    
    def __iter__(self):
        # return iter((self.func, arg) for arg in self.arguments)
        return self.iterator
    
    def get_task(self, i):
        return (self.func, self.arguments[i])
    
    def get_tasks(self):
        return [self.get_task(i) for i in range(len(self))]

    def on_completion(self, arg, resolve):
        if not isinstance(arg, collections.abc.Hashable):
            arg, *_ = arg
        try:
            result = resolve()
        except Exception as e:
            result = self.on_error(arg, e)
        else:
            result = self.on_success(arg, result)
        return result
    
    def on_error(self, arg, e):
        print('{} generated an exception: {}'.format(arg, e))
        arg_error =  '{}\nerror\n{}'.format(str_hash_arg(arg), e)
        arg_error = arg_error.encode('utf-8')
        return [arg, arg_error]
    
    def on_success(self, arg, result):
        if not isinstance(result, bytes):
            print(type(result), result)
            raise TypeError('Type of result not bytes', type(result))
        return [arg, result]


class PoolRunner:
    def __init__(self, executor_class=concurrent.futures.ThreadPoolExecutor):
        self.executor_class = executor_class
        self.results = []
        self.queue = {}
        self.finished = True
        self.completions = []

    def map(self, job, workers=5):
        with self.executor_class(max_workers=workers) as executor:
            self.submit(executor, job)
            self.join()
        return self.results
    
    def submit(self, executor, job):
        self.finished = False
        self.queue = {
            executor.submit(func, arg): (arg, job.on_completion) for func, arg in job
        }
        self.completions = self.as_completed(self.queue)
    
    def get(self):
        if not self.finished:
            future = next(self.completions)
            if future is None:
                self.finished = True
                return None
            else:
                result = self.handle_future(future)
                self.results.append(result)
                return result

    def join(self):
        for future in self.completions:
            result = self.handle_future(future)
            self.results.append(result)
        self.finished = True

    def handle_future(self, future):
        arg, result_handler = self.queue[future]
        result = result_handler(arg=arg, resolve=lambda: future.result())
        return result

    @staticmethod
    def as_completed(queue):
        return concurrent.futures.as_completed(queue)


@run_decorator
def http_request(url=None):
    if url is None:
        return default_request()
    return default_request_method(url)


@run_decorator
def shell_cmd_func(args):
    cmd, env = args
    if len(cmd) > 0:
        #if shell is not None else isinstance(cmd, str)
        completed_proc = run_subprocess(cmd, env=env)
        return completed_proc.stdout

@run_decorator
def kube_get():
    cmd = DEFAULT_KUBECTL_CMD
    return run_subprocess(cmd, shell=True)


def do_function_pool(func, arguments, pool_size=5, runner=PoolRunner()):
    this_job = Job(func=func, arguments=arguments)
    results = runner.map(this_job, workers=pool_size)
    return results


def do_shell_func_pool(command_list, shell=False, pool_size=10, executor_class=concurrent.futures.ProcessPoolExecutor):
    runner = PoolRunner(executor_class)
    return do_function_pool(func=shell_cmd_func, arguments=command_list, pool_size=pool_size, runner=runner)


def do_http_pool(url=None, num_urls=5, pool_size=5, func=http_request, runner=PoolRunner()):
    return do_function_pool(func=func, arguments=[url for i in range(num_urls)], pool_size=pool_size, runner=runner)


def do_kube_pool(pod_names, pool_size=5, func=kube_get, runner=PoolRunner()):
    return do_function_pool(func=func, arguments=pod_names, pool_size=pool_size, runner=runner)


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
    get_command = lambda i: ['/bin/bash', '-c', 'echo hello world $ENV {} ..'.format(i)]
    test_run_parallel(get_command, env={'ENV': 'dev'})


if __name__ == '__main__':
    main()
