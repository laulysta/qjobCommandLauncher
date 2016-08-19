#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import argparse
import subprocess
import logging
import time as t

from smartdispatch import utils
from smartdispatch.command_manager import CommandManager


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('commands_filename', type=str, help='File containing all commands to execute.')
    parser.add_argument('logs_dir', type=str, help="Folder where to put commands' stdout and stderr.")
    args = parser.parse_args()

    # Check for invalid arguments
    if not os.path.isfile(args.commands_filename):
        parser.error("Invalid file path. Specify path to a file containing commands.")

    if not os.path.isdir(args.logs_dir):
        parser.error("You need to specify the folder path where to put command' stdout and stderr.")

    return args


def main():
    # Necessary if we want 'logging.info' to appear in stderr.
    logging.root.setLevel(logging.INFO)

    args = parse_arguments()

    command_manager = CommandManager(args.commands_filename)

    while True:
        command = command_manager.get_command_to_run()

        if command is None:
            break

        uid = utils.generate_uid_from_string(command)
        stdout_filename = os.path.join(args.logs_dir, uid + ".out")
        stderr_filename = os.path.join(args.logs_dir, uid + ".err")

        # Get job and node ID
        try:
            job = os.environ['PBS_JOBID']
        except KeyError:
            job = 'undefined'

        try:
            with open(os.environ['PBS_NODEFILE']) as nodefile:
                node = ""
                for line in nodefile:
                    node += line + ','
                node = str.rstrip(node, ' ,\n')
        except KeyError:
            node = 'undefined'

        with open(stdout_filename, 'a') as stdout_file:
            with open(stderr_filename, 'a') as stderr_file:
                log_datetime = t.strftime("## SMART-DISPATCH - Started on: %Y-%m-%d %H:%M:%S - In job: {job} - On nodes: {node} ##\n".format(job=job, node=node))
                if stdout_file.tell() > 0:  # Not the first line in the log file.
                    log_datetime = t.strftime("\n## SMART-DISPATCH - Resumed on: %Y-%m-%d %H:%M:%S - In job: {job} - On nodes: {node} ##\n".format(job=job, node=node))

                log_command = "## SMART-DISPATCH - Command: " + command + '\n'

                stdout_file.write(log_datetime + log_command)
                stdout_file.flush()
                stderr_file.write(log_datetime + log_command)
                stderr_file.flush()

                error_code = subprocess.call(command, stdout=stdout_file, stderr=stderr_file, shell=True)

        command_manager.set_running_command_as_finished(command, error_code)

if __name__ == '__main__':
    main()
