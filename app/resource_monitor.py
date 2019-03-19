"""Classes to manage Docker containers


        Make sure that all jobs are configured to:
            write consistently formatted log statements
            flush stdout after every log statement

        Have a directory called /docker_data where all of your jobs are located

        Have all of your required docker images installed

    TODO we need to tune alpha and time interval for each model
"""
import re
import subprocess
import time
import warnings
from multiprocessing import cpu_count
import logging

import pandas as pd

from app.repeated_timer import RepeatedTimer
from utils import get_logger

logger = get_logger(__name__)


class ResourceMonitor(object):
    """An object that maintains a table of docker resource usage statistics

    Meant to be used as a singleton.

    Runs `docker stats --no-stream` every n seconds using a RepeatedTimer object, accumulating results into a DataFrame
    """

    def __init__(self, update_interval=10):
        """
        :param update_interval: how frequently, in seconds, to update docker stats table
        """
        logger.info('Initializing ResourceMonitor with update interval = {}'.format(update_interval))
        self.history = self._check_stats()
        self._update_interval = update_interval
        self._timer = RepeatedTimer(interval=self._update_interval, function=self._update)

    @staticmethod
    def _check_stats():
        """Run `docker stats --no-stream` and parse into pd.DataFrame

        TODO the columns printed vary with docker versions... standardize this somehow.
        TODO note: had to install docker version 17 and anaconda on chameleon for this to work
        """

        logger.info('ResourceMonitor: checking stats')
        columns = ['container_id', 'cpu_pct', 'mem_use', 'mem_max',
                   'mem_pct', 'net_in', 'net_out', 'block_in', 'block_out', 'pids']

        records = subprocess.check_output(['docker', 'stats', '--no-stream']).decode('ascii')
        records = records.split('\n')[1:-1]  # exclude headers and trailing empty string
        records = [re.split('[ /]+', record) for record in records]

        stats = pd.DataFrame.from_records(records, columns=columns)
        stats['time'] = time.time()
        logger.info('ResourceMonitor: done checking stats')
        return stats

    def cpu_mean(self, id, interval):
        """
        Calculates cpu_mean of each container in self.history
        :param id:
        :param interval:
        :return: the cpu_mean of each container in self.history
        """
        resources = self.history[self.history.container_id == id]
        resources = resources[resources.time >= (time.time() - interval)]

        if resources.empty:
            warn_str = "No resources history in this interval for container: {}, returning cpu_mean of None".format(id)
            warnings.warn(warn_str, RuntimeWarning)
            logger.warning(warn_str)
            return None

        resources['cpu_pct'] = resources.cpu_pct.str.rstrip('%').astype(float)
        resources['cpu_norm'] = resources.cpu_pct / cpu_count() / 100
        resources['mem_norm'] = resources.mem_pct.str.rstrip('%').astype(float) / 100
        return resources.cpu_norm.mean()

    def start(self):
        self._timer.start()

    def _update(self):
        """Run self._check_stats() and concatenate to self.history"""
        self.history = pd.concat([self.history, self._check_stats()], ignore_index=True)

    def stop(self):
        """Stop the RepeatedTimer thread"""
        self._timer.stop()

    def to_csv(self, experiment_name):
        """Save self.history to a csv

        :param experiment_name: the name of the controlling Trial instance
        :return: None
        """
        logger.info("Writing ResourceMonitor table to csv")
        self.history.to_csv("{}_docker_stats.csv".format(experiment_name), index=False)
