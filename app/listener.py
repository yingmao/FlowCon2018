import logging

import utils
from app.repeated_timer import RepeatedTimer
from app.algorithm import *

logger = utils.get_logger(__name__)

class BackoffListener(object):

    def __init__(self, trial, interval=10):
        """Listen for new containers, manipulate a Trial's timer"""
        self._is_running = False
        self.trial = trial
        self.timer = RepeatedTimer(interval, self.listen)
        self.active_containers = []

    def start(self):
        # Keep the listener from being started twice
        if not self._is_running:
            logger.info("starting BackoffListener")
            self._is_running = True
            self.active_containers = utils.get_active_containers()
            self.timer.start()

    def stop(self):
        logger.info("stopping BackoffListener")
        self.timer.stop()
        self._is_running = False

    def listen(self):
        logger.info("running BackoffListener.listen()")
        current_active = utils.get_active_containers()
        if len(current_active) == 0:
            logger.info("no containers detected")
            self.stop()
            self.trial.stop()
        else:
            # Reset the trial timer if there is a new container
            for container in current_active:
                if container not in self.active_containers:
                    logger.info('new container detected')
                    self.trial.stop_backoff()
            for container in self.active_containers:
                if container not in current_active:
                    logger.info('container stop detected')
                    self.trial.stop_backoff()
