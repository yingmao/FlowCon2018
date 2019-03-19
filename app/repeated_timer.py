"""Classes to handle threading
"""

from threading import Timer
import logging

from utils import get_logger

logger = get_logger(__name__)


class RepeatedTimer(object):
    """
    Executes a function with arbitrary arguments every `interval` seconds

    from: https://stackoverflow.com/questions/3393612/run-certain-code-every-n-seconds
    """
    def __init__(self, interval=30, function=None, *args, **kwargs):
        """
        :param interval: the number of seconds to wait before calling `function` again
        :param function: the function to execute every `interval` seconds
        :param args: positional arguments to `function`
        :param kwargs: keyword arguments to `function`
        """
        logger.info("Initializing RepeatedTimer instance with function: {}, interval: {}".format(function.__name__,
                                                                                                 interval))
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self.is_running = False
        self.is_running = False

    def _run(self):
        logger.info("Executing RepeatedTimer._run with function: {}".format(self.function.__name__))
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        logger.info("Starting RepeatedTimer._timer")
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        logger.info("Stopping RepeatedTimer._Timer object")
        self._timer.cancel()
        self.is_running = False
