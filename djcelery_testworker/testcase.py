import os
import threading

from django.test import TransactionTestCase
from django.conf import settings
from . import run_celery_test_worker

class CeleryWorkerThread(threading.Thread):
    """
    Thread for running a live celery worker in the background
    """

    def __init__(self, options=[]):
        self.error = None
        self.is_ready = threading.Event()
        self.options = options
        
        super(CeleryWorkerThread, self).__init__()

    def run(self):
        """
        Sets up the celery worker
        """
        try:
            # Start the worker
            self.process = run_celery_test_worker(self.options)
            
        except Exception as e:
            # Set the error and fire signal
            import traceback
            traceback.print_exc()
            self.error = e
            return

        # Signal ready and wait for worker process to terminate
        self.is_ready.set()
        self.process.wait()


    def join(self, timeout=None):
        """
        Quits the celery worker
        """
        if hasattr(self, 'process'):
            try:
                self.process.kill()
            except OSError:
                pass

        # Join with main thread
        super(CeleryWorkerThread, self).join(timeout)



class CeleryWorkerTestCase(TransactionTestCase):
    """
    Does basically the same as TransactionTestCase but also launches a live
    celery worker in a separate thread so that we can run feature tests that
    depend on a celery worker being run. 

    Note that it inherits from TransactionTestCase instead of TestCase because
    the threads do not share the same transactions (unless if using in-memory
    sqlite) and each thread needs to commit all their transactions so that the
    other thread can see the changes.
    """

    @classmethod
    def setUpClass(cls):
        # need to set this here so the server also uses this broker to send tasks to
        if hasattr(settings, "CELERY_TEST_BROKER"):
            settings.BROKER_URL = settings.CELERY_TEST_BROKER
            os.environ['CELERY_BROKER_URL'] = settings.CELERY_TEST_BROKER

        super(CeleryWorkerTestCase, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        # There may not be a 'server_thread' attribute if setUpClass() for some
        # reasons has raised an exception.
        if hasattr(cls, 'worker_thread'):
            # Terminate the worker's thread
            cls.worker_thread.join(5)

        super(CeleryWorkerTestCase, cls).tearDownClass()

    def _pre_setup(self):
        self.worker_thread = CeleryWorkerThread()
        self.worker_thread.start()

        # Wait for the worker to be ready
        self.worker_thread.is_ready.wait()
        if self.worker_thread.error:
            raise self.worker_thread.error
        super(CeleryWorkerTestCase, self)._pre_setup()

    def _post_teardown(self):
        self.worker_thread.join(5)
        super(CeleryWorkerTestCase, self)._post_teardown()
