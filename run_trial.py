"""The main point of entry for this program"""

import argparse
import subprocess
import time
from subprocess import DEVNULL
import warnings
import logging

import pandas as pd

import utils
from app.trial import Trial
from utils import get_logger

# make sure log is empty, so it only reflects this session
with open("FlowCon.log", "w+") as f:
    f.truncate()
logger = get_logger(__name__)


def run_job_list(job_list):
    """"""
    jobs = pd.read_csv(job_list)
    stop = jobs.seconds.max()

    for i in range(stop+1):
        jobs_i = jobs[jobs.seconds == i]
        # print('i:', i, '\njobs_i:\n', jobs_i)
        if jobs_i.shape[0] > 0:
            for job in jobs_i.images:
                subprocess.Popen(['docker', 'run', job], stdout=DEVNULL)
                logger.info('Launching container with `docker run {}`'.format(job))
        time.sleep(1)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('joblist', help='A csv of jobs to run')
    parser.add_argument('-i', '--interval', type=int, default=30,
                        help='The interval at which to run algorithm 1')
    parser.add_argument('-a', '--alpha', type=float, default=0.03,
                        help='Rate at which to change resource allocation')
    parser.add_argument("--docker_stats_interval", type=float, default=10,
                        help="Number of seconds between calls to `docker stats`")
    control = parser.add_mutually_exclusive_group()
    control.add_argument('--no_update', action='store_true',
                         help='Run the algorithm but do not update any container limits')
    control.add_argument('--no_algo', action='store_true',
                         help='Do not run the algorithm')
    control.add_argument('--no_backoff', action='store_true',
                         help='Do not run the backoff listener')

    args = parser.parse_args()
    for arg, val in vars(args).items():
        logger.info("Argument {}: {}".format(arg, val))

    if args.docker_stats_interval > args.interval:
        warnings.warn("Ensure that docker_stats_interval is less than interval...\n"
                      "Reducing docker_stats_interval to interval/2: {}".format(args.interval/2))
        args.docker_stats_interval = args.interval/2

    active_containers = utils.get_active_containers()
    if len(active_containers) > 0:
        valid = False
        while not valid:
            logger.info("Found {} active containers on system, prompting user to kill them..."
                        .format(len(active_containers)))
            kill = input("There are {} active containers, would you like to kill them? (y/n): "
                         .format(len(active_containers)))
            valid = kill == 'y' or kill == 'n'
        if kill == 'y':
            logger.info("Killing active containers as directed by user")
            utils.kill_containers_by_id(active_containers)
        else:
            logger.info("User refused to kill containers, exiting")
            raise RuntimeError("Experiment may not be valid if other containers are active.")

    session_name = "no_algo" if args.no_algo \
                   else "no_update" if args.no_update \
                   else "a{}_i{}".format(args.alpha, args.interval)
    logger.info(
        "Running trial with arguments a = {}, i = {}, name = {}".format(args.alpha, args.interval, session_name))
    start_time = time.time()
    logger.info("Session start time: {}".format(start_time))
    trial = Trial(interval=args.interval, name=session_name, alpha=args.alpha, no_algo=args.no_algo,
                  no_update=args.no_update, stats_interval=args.docker_stats_interval, start_time=start_time,no_backoff=args.no_backoff)
    trial.start()
    run_job_list(args.joblist)
