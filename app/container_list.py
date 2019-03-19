import subprocess
import logging

from app.container_wrapper import ContainerWrapper
from utils import get_logger

logger = get_logger(__name__)


class ContainerList(object):
    """A list-like object for storing ContainerWrappers"""

    def __init__(self, trial_start, interval, no_update=False, *args):
        """Create self from a comma-separated list of ContainerWrappers
        :param *args: ContainerWrapper objects to store in instance
        """
        logger.info("Initializing ContainerList")
        self.no_update      = no_update
        self.containers     = []
        self.interval       = interval
        self.trial_start    = trial_start
        self.add(*args)

    def add(self, *args):
        """Add more containers to self

        :param *args: ContainerWrapper objects to store in instance
        :return: None
        """
        for arg in args:
            if not isinstance(arg, ContainerWrapper):
                logger.error("ContainerList passed non-ContainerWrapper object")
                raise ValueError("ContainerList can only take ContainerWrapper objects, got {}".format(type(arg)))

        # logger.info("Adding {} containers to ContainerList".format(len(args)))
        self.containers.extend(list(args))

    def reconcile(self, experiment_name, no_update=False):
        """Reconcile the state of the container list with the state of currently active containers

        Add any newly created containers running on the machine to self, and remove those that have terminated

        :param experiment_name: the name of the controlling Trial instance
        :return: None
        """

        logger.info('Reconciling ContainerList with docker ps')

        active_containers = subprocess.check_output(['docker', 'ps', '-q']).decode('ascii').split('\n')[:-1]

        for c_id in active_containers:
            if c_id not in self.ids:
                c = ContainerWrapper(id=c_id, updatable=not no_update,
                                     trial_start=self.trial_start, interval=self.interval)
                logger.info('Adding {} to ContainerList'.format(c_id))
                self.add(c)

        for c in self:
            if c.id not in active_containers:
                logger.info('Removing {} from ContainerList'.format(c.id))
                c.save_logs(experiment_name=experiment_name)
                self.containers.remove(c)

    def __iter__(self):
        for container in self.containers:
            yield container

    def __len__(self):
        return len(self.containers)

    def killall(self, experiment_name, save_logs=True):
        """Kill all ContainerWrappers in self"""
        for container in self:
            if save_logs:
                container.save_logs(experiment_name=experiment_name)
            container.kill()

    @property
    def all_completing(self):
        """Check if all containers in self have been marked as 'completing' by the algorithm
        :return: bool
        """
        for container in self:
            if not container.completing:
                return False
        return True

    @property
    def num_completing(self):
        """Return the number of containers in self that the algorithm has marked as completing"""
        return sum(1 for c in self if c.completing)

    @property
    def num_watching(self):
        """Return the number of containers in self that the algorithm has marked as completing"""
        return sum(1 for c in self if c.watching)

    @property
    def ids(self):
        """Return a list of container IDs corresponding to the containers stored in self"""
        return [c.id for c in self]
