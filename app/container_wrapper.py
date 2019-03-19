import re
import subprocess
import time
from multiprocessing import cpu_count
from subprocess import DEVNULL
import logging

import numpy as np
import pandas as pd

from utils import get_logger

logger = get_logger(__name__)


class ContainerWrapper(object):
    """A python interface to docker containers running ML jobs

    Allows us to monitor the state of evaluation functions and update resource limits.
    """

    def __init__(self, trial_start, interval, id=None, njobs=1, updatable=True):
        """
        :param id: Container ID: if create=True then this has no effect
        :param create: if True, the ContainerWrapper will create a container based on `image`, `wd`, and `script`
        :param image: see `create`
        :param wd: see `create`
        :param script: see `create`
        :param njobs: number of ML jobs running within the container. Currently only supports 1.
        :param updatable: determines if we can apply resource updates to this container
        """
        self.id             = id
        self.updatable      = updatable
        self.mem_lim        = None
        self.cpu_lim        = cpu_count()
        self.njobs          = njobs
        self.watching       = None   # TODO: In my option, these properties are pretty sloppy OO.
        self.completing     = None   # They are essentially using a ContainerWrapper object to store data for logic
        self.frozen         = False  # external to the container object leading to class bloat
        self._creation_time = time.time()
        self._last_checked  = 0
        self.__E_i           = 0
        self.__E_i_minus_1   = 0
        self.trial_start    = trial_start
        self.interval       = interval
        if njobs != 1:
            raise NotImplementedError('Currently only supports one job')

    @property
    def _complete_loss_logs(self):
        """Parse the container logs and return a pd.DataFrame of the loss function over the lifetime of the container"""

        if self.njobs == 1:
            logs = subprocess.check_output(['docker', 'logs', self.id])
            logs = logs.split(b"\n")
            loss = []
            timestamp = []
            for line in logs[:-1]:
                try:
                    l = float(re.search(b'Loss: ([0-9.]+)', line).group(1))
                    t = float(re.search(b'Time: ([0-9.]+)', line).group(1))
                except AttributeError:  # If re.search returns NoneType, which has no attribute 'group'
                    continue
                else:
                    loss.append(l)
                    timestamp.append(t)

            history = pd.DataFrame({'loss': loss, 'time': timestamp})

        else:
            raise NotImplementedError("This should never happen: currently only supports one job")
            # When more than one job is supported, this method will have to change

        return history

    @property
    def cpu_lim(self):
        """CPU limit placed on container where the unit is the number of cpus

        Setting cpu_lim causes an instance to run `docker update self.id --cpus limit`
        Always returns 1 if not self.updatable (trivially the container always has full access if no limits are applied)
        """
        return self._cpu_lim if self.updatable else 1

    @cpu_lim.setter
    def cpu_lim(self, limit):
        if self.updatable:
            logger.info("Setting container {} cpu limit to {}".format(self.id, limit))
            response = subprocess.check_output(['docker', 'update', '--cpus', str(int(limit)), self.id])
            logger.info("Docker response: {}".format(response))
            self._cpu_lim = limit

    @property
    def age(self):
        return time.time() - self.trial_start

    def _compute_loss(self):
        """Compute the loss over this interval and the previous interval as described in the paper"""
        logger.info('Computing mean loss over intervals i and i-1')
        now = time.time()
        interval = self.interval
        loss_logs = self._complete_loss_logs
        loss = loss_logs['loss']
        times = loss_logs['time']
        loss = loss/loss.max()
        # See writeup of Algorithm 1 in paper to disambiguate notational choices here
        loss_over_this_interval = loss[times >= now - interval]
        loss_over_last_interval = loss[(now - 2 * interval <= times) & (times <= now - interval)]
        logger.info('Num observations over this interval: {}'.format(len(loss_over_this_interval)))
        logger.info('Num observations over last interval: {}'.format(len(loss_over_last_interval)))
        self.__E_i = loss_over_this_interval.mean()
        self.__E_i_minus_1 = loss_over_last_interval.mean()
        logger.info("Set self.__E_i to {}".format(self.__E_i))
        logger.info("Set self.__E_i_minus_1 to {}".format(self.__E_i_minus_1))

    @property
    def E_i(self):
        logger.info("Checking self.E_i")
        delta_t = time.time() - self._last_checked
        if delta_t > self.interval:
            logger.info("Time since checked: {}".format(delta_t))
            self._compute_loss()
            self._last_checked = time.time()
        return self.__E_i

    @property
    def E_i_minus_1(self):
        logger.info("Checking self.E_i_minus_1")
        delta_t = time.time() - self._last_checked
        if abs(delta_t - self.interval) < 2.0:
            logger.info("Time since checked: {}".format(delta_t))
            self._compute_loss()
            self._last_checked = time.time()
        self._last_checked = time.time()
        return self.__E_i_minus_1

    @property
    def progress(self):
        logger.info("Running self.progress")
        E_i = self.E_i
        E_i_minus_1 = self.E_i_minus_1
        if np.isnan(E_i_minus_1) or np.isnan(E_i):
            return 0
        logger.info("Computing progress with abs({} - {}) / {}".format(self.__E_i, self.__E_i_minus_1, self.interval))
        return abs(self.E_i - self.E_i_minus_1) / self.interval

    def growth(self, monitor, threshold=0):
        logger.info("Running self.growth")
        if threshold > 0:
            raise NotImplementedError("We haven't implemented anything for threshold > 0, got threshold = {}".format(threshold))

        E_i_minus_1 = self.E_i_minus_1

        if np.isnan(E_i_minus_1):
            logger.warning("E_i_minus_1 is NaN")
            return 0

        cpu_mean = monitor.cpu_mean(self.id, self.interval)
        if cpu_mean is None:
            logger.warning("No cpu mean")
            return 0

        logger.info("Computing growth with {} / {}".format(self.progress, cpu_mean))
        return self.progress / cpu_mean

    def save_logs(self, experiment_name):
        """Save loss function table to csv

        :param experiment_name: the name of the controlling Trial object
        :return: None
        """
        table = self._complete_loss_logs
        logger.info("Saving logs for container {}".format(self.id))
        table.to_csv("{}_{}.csv".format(experiment_name, self.id), index=False)

    def kill(self):
        """Kill the container controlled by self"""
        subprocess.run(['docker', 'container', 'kill', self.id], stdout=DEVNULL)
