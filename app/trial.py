"""The Trial class, a singleton which manages given experiment.

The Trial.run() method can be thought of the top-level 'main' method. The reason it is encapsulated inside of
a Trial object rather than defined as a global function is as follows: the run method needs to be executed
repeatedly over a specific interval. So it needs to have an associated RepeatedTimer object. The Trial class keeps
the run method and the timer bound together. Further, it provides a place for the results of each iteration of algorithm 1
to accumulate.


"""

import glob
import os
import shutil
import sys
import zipfile
import logging

from app.algorithm import *
from app.container_list import ContainerList
from app.listener import BackoffListener
from app.repeated_timer import *
from utils import get_logger

logger = get_logger(__name__)


class Trial(object):
    """Manage one experimental run, called a Trial:
        repeatedly run the algorithm at a given time interval,
        save algorithm logs and resource monitor logs once no containers are active on the host machine

        Note that Trial will do its best to avoid duplicate
        experiment names by checking its name against logs left
        over by previous experiments. If it's name appears to be a duplicate, it will raise a ValueError

    TODO Ideal case: each container has one monitor
    """

    def __init__(self, alpha, name, interval, start_time, stats_interval, no_algo=False, no_update=False,
                 no_backoff=False, beta=1.2):
        """
        :param interval: the interval at which to run algorithm 1
        :param alpha: alpha for altorithm 1
        :param name: A name for the experiment Trial, passed as a command line arg.
        :param stats_interval: number of seconds between calls to docker stats: passed to ResourceMonitor
        """

        if glob.glob('./{}*.zip'.format(name)):
            raise ValueError("Logs zip for an experiment with name '{}' already exists, ".format(name) +
                             "please use unique experiment names")

        self.interval                = interval
        self.alpha                   = alpha
        self.beta                    = beta
        self.name                    = name
        self.monitor                 = ResourceMonitor(stats_interval)
        self.containers              = ContainerList(trial_start=start_time, interval=interval)
        self.containers.no_update    = no_update
        self.status                  = None
        self.interval                = interval
        self.backoff_interval        = interval  # for the exponential backoff
        self.stats_interval          = stats_interval
        self.iter_num                = 0
        self.start_time              = start_time
        self.no_algo                 = no_algo
        self.no_update               = no_update
        self.no_backoff              = no_backoff
        self.listener                = BackoffListener(self)
        self.timer                   = RepeatedTimer(self.interval, self.run)
        self.last_run                = None  # for computing s_since_last_run inside of algo_1

        logger.info("Created Trial object with parameters name = {}, alpha = {}, beta={}, interval = {},"\
                    .format(name, alpha, beta, interval))

    def backoff(self):
        if self.no_backoff:
            return
        self.backoff_interval *= 2
        logger.info("Backing off algo interval to {}".format(self.backoff_interval))
        self.timer.stop()
        self.timer = RepeatedTimer(self.backoff_interval, self.run)
        self.timer.start()
        self.listener.start()

    def stop_backoff(self):
        if self.no_backoff:
            return
        self.timer.stop()
        self.listener.stop()
        logger.info("stopping backoff")
        self.run()
        self.backoff_interval = self.interval
        logger.info("Resetting algo interval to {}".format(self.interval))
        self.timer = RepeatedTimer(self.interval, self.run)
        self.timer.start()

    def run(self):
        """The main procedure of an Trial

        :param containers: the ContainerList
        :param monitor: the DockerMonitor
        :return: None

        This gets executed by self.timer every self.interval seconds

        In pseudocode:

            check for new containers and terminated containers:
                update ContainerList and save logs accordingly
            run algorithm 1 over the ContainerList
            append the results of algorithm1 to self.status
            write the cardinality of containers in (watching, completing, and total) to the appropriate log
            update ContainerList
            if ContainerList is empty:
                save all logs
                zip all logs
                exit
        """
        logger.info("Executing Trial.run()")
        self.containers.reconcile(experiment_name=self.name)
        if not self.no_algo and len(self.containers) > 0:
            beta = 1 + 1/len(self.containers)
            status = algo_1(self.containers, self.monitor, alpha=self.alpha, beta=beta, interval=self.interval,
                            last_run=self.last_run)
            self.last_run = time.time()

            status.insert(2, 'iter', self.iter_num)
            self.iter_num += 1
            status['backoff_interval'] = self.backoff_interval
            
            print(status)

            if self.status is None:
                self.status = status
            else:
                self.status = pd.concat([self.status, status])

            if self.containers.all_completing and not self.no_backoff:
                self.backoff()

        self.containers.reconcile(experiment_name=self.name)

        if len(self.containers) == 0:
            self.stop()

    def to_csv(self):
        logger.info("Writing Trial records to CSV")
        if not self.no_algo:
            self.status.to_csv('{}_algo_1_iters.csv'.format(self.name), index=False)
        self.monitor.to_csv(self.name)

    def start(self):
        self.monitor.start()
        self.timer.start()

    def stop(self):
        logger.info('Killing Trial Instance')
        self.containers.killall(self.name)
        self.to_csv()
        self.zip_logs()
        self.timer.stop()
        self.monitor.stop()
        sys.exit(0)

    def zip_logs(self):
        """Move all log files to a separate directory, zip them and delete the raw log files"""
        new_dir = "./{}".format(self.name)
        logger.info("Zipping Trial records")
        os.makedirs(new_dir)
        shutil.move("FlowCon.log", new_dir)
        for file in glob.glob("{}*".format(self.name)):
            shutil.move(file, new_dir)

        with zipfile.ZipFile('{}_logs.zip'.format(self.name), 'w', zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(new_dir):
                for file in files:
                    zf.write(os.path.join(root, file))

        shutil.rmtree(new_dir)
