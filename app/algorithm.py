"""This module implements algorithm 1 from the paper
"""

import multiprocessing
import time

import utils
from app.resource_monitor import *
import logging

logger = utils.get_logger(__name__)


def algo_1(containers, monitor, alpha, beta, interval, last_run):
    """Run algorithm1 over a ContainerList
    :param containers: the ContainerList for the session
    :param monitor: the DockerMonitor for the session
    :param alpha: decision threshold for growth efficiency
    :param beta: weight for old containers vs new containers
    :param interval: time interval over which to run the algorithm
    :return: a pandas DF of the status of all monitored containers after the run of the algorithm

    TODO refactor such that interval and alpha can vary independently for each container
    """


    logger.info("Running algorithm 1 with parameters alpha = {}, interval = {}".format(alpha, interval))

    delta_t = 0 if last_run is None else round(time.time() - last_run, 2)

    # accumulators for pandas DF
    growth = [0] * len(containers)
    loss = [0] * len(containers)
    progress = [0] * len(containers)
    ages = [0] * len(containers)
    ignore = [False] * len(containers)

    all_completing = True
    for i, c in enumerate(containers):
        all_completing = False if not c.completing else all_completing

        growth[i] = c.growth(monitor)
        loss[i] = c.E_i
        progress[i] = c.progress
        ages[i] = c.age

        G = growth[i]
        if G < alpha and not c.completing:
            logger.info("Marking {} as watching".format(c.id))
            if c.watching:
                logger.info("Marking {} as completing".format(c.id))
                c.completing = True
            else:
                c.completing = False
            c.watching = not c.watching
        elif G >= alpha:
            logger.info("Marking {} as neither watching nor completing".format(c.id))
            c.watching = c.completing = False

    new_lim = multiprocessing.cpu_count()
    if all_completing and len(containers) != 0:
        for c in containers:
            c.cpu_lim = multiprocessing.cpu_count()

    else:  # if containers.has_growing:
        # Then we still have some containers that are growing fast
        # Apply resource limits from lines 16-22 of the algorithm as written in the paper
        growth_sum = sum(growth)
        logger.info("Value for growth sum: {:.3f}".format(growth_sum))
        for i, c in enumerate(containers):
            if growth_sum == 0:
                break
            if not c.watching:
                growth_ratio = growth[i] / growth_sum
                if c.completing:
                    new_lim = max(growth_ratio, 1 / (beta * len(containers)))
                else:
                    new_lim = min(growth_ratio, 1)
            if len(containers) == 0:
                break
            # else:
            #     new_lim = min(new_lim, 1)

            if not c.watching:
                c.cpu_lim = new_lim * multiprocessing.cpu_count()

    now = time.time()
    limits = [c.cpu_lim for c in containers]
    W = [c.watching for c in containers]
    C = [c.completing for c in containers]
    ids = [c.id for c in containers]
    num_containers = len(containers)
    num_watching = containers.num_watching
    num_completing = containers.num_completing

    status = pd.DataFrame(dict(
        time=now,
        c_id=ids,
        age=ages,
        ignore=ignore,
        loss=loss,
        progress=progress,
        growth=growth,
        limit=limits,
        watching=W,
        completing=C,
        delta_t=delta_t,
        num_containers=num_containers,
        num_watching=num_watching,
        num_completing=num_completing,
        beta=beta
    ))
    status = status[['time', 'age', 'ignore', 'c_id', 'loss', 'progress', 'growth', 'limit', 'watching', 'completing',
                     'delta_t', 'num_containers', 'num_watching', 'num_completing', 'beta']]

    normalized_limit = status['limit'] / multiprocessing.cpu_count()
    status.insert(8, 'limit_norm', normalized_limit)
    return status
