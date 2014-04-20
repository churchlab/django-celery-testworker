import os
import subprocess
import threading
import sys
from threading import Timer
from django.conf import settings

class WorkerOutputThread(threading.Thread):
    def __init__(self, pty, process, silent=True):
        self.pty = pty
        self.process = process
        self.is_ready = threading.Event()
        self.close = False
        self.silent = silent
        self.output = []
        super(WorkerOutputThread, self).__init__()
        # if we kill the process, this thing will hang on the readline()
        self.daemon = True

    def run(self):
        stdout = os.fdopen(self.pty)

        while not self.process.poll() and not self.close:
            line = stdout.readline()
            if line:
                self.output.append(line)
            
                # Wait for the daemon to be ready (it'll print a line with a broker url to stdout)
                # TODO: monitor output of process to detect failure
                if "ready" in line:
                    self.is_ready.set()

                # HACK: When an exception is raised, write the exception,
                # then sleep until failure state is set on celery task, then
                # kill the celery process. The reason I added this hack is that
                # the test just hangs after celery has errored. There's
                # probably a place to check celery's status, but I havne't
                # found it.
                if "raised exception" in line:
                    sys.stdout.write(line)
                    import time
                    time.sleep(5)
                    self.process.kill()
                    break

                if line != "" and not self.silent:
                    sys.stdout.write(line)

def run_celery_test_worker(options=[]):
    """
    Starts a celery worker that operates on the test database in the background and 
    waits for it to be ready. Returns process handle.
    """
    import pty, sys

    if hasattr(settings, "CELERY_TEST_BROKER"):
        settings.BROKER_URL = settings.CELERY_TEST_BROKER
        os.environ['CELERY_BROKER_URL'] = settings.CELERY_TEST_BROKER

    silent=True
    if '--verbose' in options:
        options.remove('--verbose')
        silent = False    
    
    execv_argv = ["./manage.py", "celerytestworker"] # celerytestworker command defined by celerytests

    # python buffers stdout normally, use pty instead
    master, slave = pty.openpty()
    process = subprocess.Popen(execv_argv + options, stdin=subprocess.PIPE, stdout=slave, stderr=slave, close_fds=True)

    output_thread = WorkerOutputThread(master, process, silent=silent)
    output_thread.start()
    if not output_thread.is_ready.wait(5):
        output_thread.close = True
        raise Exception("Celery worker failed: " + "\n".join(output_thread.output))

    if process.returncode is not None:
        output_thread.close = True
        raise Exception("Celery worker failed to start")

    return process
