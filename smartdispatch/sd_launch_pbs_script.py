import argparse
import logging

from smartdispatch import launch_jobs
from smartdispatch import utils

LOGS_FOLDERNAME = "SMART_DISPATCH_LOGS"
CLUSTER_NAME = utils.detect_cluster()
LAUNCHER = utils.get_launcher(CLUSTER_NAME)


def main():
    # Necessary if we want 'logging.info' to appear in stderr.
    logging.root.setLevel(logging.INFO)

    args = parse_arguments()

    launch_jobs(LAUNCHER if args.launcher is None else args.launcher, [args.pbs], CLUSTER_NAME, args.path_job)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-L', '--launcher', choices=['qsub', 'msub'], required=False, help='Which launcher to use. Default: qsub')
    parser.add_argument('pbs', type=str, help='PBS filename to launch.')
    parser.add_argument('path_job', type=str, help='Path to the job folder.')

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    main()
