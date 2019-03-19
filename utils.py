import subprocess
import logging


# TODO set this up at some point. removes clutter from main
# def get_user_input(message, options, yes_no=True, logger=None, log_msgs=None, error=None, error_msg=None):
#     user_input = input(message)
#     while user_input not in options:
#         print("That is not a valid response. The valid responses are {}. Please try again.".format(options))
#         user_input = input(message)
#     if yes_no:
#         if user_input == "yes" or user_input == "y":
#             if logger is not None:
#                 logger.info(log_msgs[0] if len(log_msgs) > 0 else log_msgs)
#                 return user_input
#         else:
#             if logger is not None:


def kill_all_active_containers():
    subprocess.run("docker container kill $(docker ps -q)")
    return True


def kill_containers_by_id(active_containers):
    for c_id in active_containers:
        subprocess.run("docker container kill {}".format(c_id), shell=True)
    return True


def get_active_containers():
    """Return the number of currently running containers"""
    out = subprocess.check_output(['docker', 'ps', '-q'])
    out = out.decode('ascii')
    out = out.split('\n')
    active_containers = []
    for line in out:
        if line != '':
            active_containers.append(line)
    return active_containers


def get_logger(name, fn='FlowCon.log', fmt='%(asctime)s:%(levelname)s:%(name)s:%(message)s'):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(fmt)
    fh = logging.FileHandler(fn)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger
