#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re
import numpy as np
import argparse
import datetime
import math
from subprocess import check_output

import smartdispatch.utils as utils


AVAILABLE_QUEUES = {
    # Mammouth Parallel
    'qtest@mp2': {'coresPerNode': 24, 'maxWalltime': '00:01:00:00'},
    'qwork@mp2': {'coresPerNode': 24, 'maxWalltime': '05:00:00:00'},
    'qfbb@mp2': {'coresPerNode': 288, 'maxWalltime': '05:00:00:00'},
    'qfat256@mp2': {'coresPerNode': 48, 'maxWalltime': '05:00:00:00'},
    'qfat512@mp2': {'coresPerNode': 48, 'maxWalltime': '02:00:00:00'},

    # Mammouth Série
    'qtest@ms': {'coresPerNode': 8, 'maxWalltime': '00:01:00:00'},
    'qwork@ms': {'coresPerNode': 8, 'maxWalltime': '05:00:00:00'},
    'qlong@ms': {'coresPerNode': 8, 'maxWalltime': '41:16:00:00'},

    # Mammouth GPU
    # 'qwork@brume' : {'coresPerNode' : 0, 'maxWalltime' : '05:00:00:00'} # coresPerNode is variable and not relevant for this queue
}


def main():
    args = parse_arguments()

    if args.commandsFile is not None:
        # Commands are listed in a file.
        jobname = args.commandsFile.name
        commands = get_commands_from_file(args.commandsFile)
    else:
        # Commands that needs to be parsed and unfolded.
        arguments = []
        for opt in args.commandAndOptions:
            opt_split = expand_argument(opt)

            for i, split in enumerate(opt_split):
                opt_split[i] = os.path.normpath(split)  # If the arg value is a path, remove the final '/' if there is one at the end.

            arguments += [opt_split]

        jobname = generate_name(arguments)
        commands = get_commands_from_arguments(arguments)

    job_directory, qsub_directory = create_job_folders(jobname)

    # Pool of workers
    if args.pool is not None:
        commands_filename = os.path.join(qsub_directory, "commands.txt")
        with open(commands_filename, 'w') as f:
            f.write("\n".join(commands))

        worker_command = 'smart_worker.py "{0}" "{1}"'.format(commands_filename, job_directory)
        # Replace commands with `args.pool` workers
        commands = [worker_command] * args.pool

    # Distribute equally the jobs among the QSUB files and generate those files
    nb_commands = len(commands)
    nb_jobs = int(math.ceil(nb_commands / float(args.nbCommandsPerNode)))
    nb_commands_per_file = int(math.ceil(nb_commands / float(nb_jobs)))

    qsub_filenames = []
    for i, commands_per_file in enumerate(utils.chunks(commands, n=nb_commands_per_file)):
        qsub_filename = os.path.join(qsub_directory, 'jobCommands_' + str(i) + '.sh')
        write_qsub_file(commands_per_file, qsub_filename, job_directory, args.queueName, args.walltime, os.getcwd(), args.cuda)
        qsub_filenames.append(qsub_filename)

    # Launch the jobs with QSUB
    if not args.doNotLaunch:
        for qsub_filename in qsub_filenames:
            qsub_output = check_output('qsub ' + qsub_filename, shell=True)
            print qsub_output,


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queueName', required=True, help='Queue used (ex: qwork@mp2, qfat256@mp2, qfat512@mp2)')
    parser.add_argument('-t', '--walltime', required=False, help='Set the estimated running time of your jobs using the DD:HH:MM:SS format. Note that they will be killed when this time limit is reached.')
    parser.add_argument('-n', '--nbCommandsPerNode', type=int, required=False, help='Set the number of commands per nodes.')
    parser.add_argument('-c', '--cuda', action='store_true', help='Load CUDA before executing your code.')
    parser.add_argument('-x', '--doNotLaunch', action='store_true', help='Creates the QSUB files without launching them.')
    parser.add_argument('-f', '--commandsFile', type=file, required=False, help='File containing commands to launch. Each command must be on a seperate line. (Replaces commandAndOptions)')
    parser.add_argument('--pool', type=int, help="Number of workers that will consume commands.")
    parser.add_argument("commandAndOptions", help="Options for the command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    # Check for invalid arguments
    if args.commandsFile is None and len(args.commandAndOptions) < 1:
        parser.error("You need to specify a command to launch.")
    if args.queueName not in AVAILABLE_QUEUES and (args.nbCommandsPerNode is None or args.walltime is None):
        parser.error("Unknown queue, --nbCommandsPerNode and --walltime must be set.")

    # Set queue defaults for non specified params
    if args.nbCommandsPerNode is None:
        args.nbCommandsPerNode = AVAILABLE_QUEUES[args.queueName]['coresPerNode']
    if args.walltime is None:
        args.walltime = AVAILABLE_QUEUES[args.queueName]['maxWalltime']

    return args


def get_commands_from_file(fileobj):
    return fileobj.read().split('\n')


def expand_argument(argument):
    stringify = lambda array: [str(e) for e in array]

    if argument.startswith("range(") or argument.startswith("linspace(") or \
       argument.startswith("logspace(") or argument.startswith("randint(") or \
       argument.startswith("uniform(") or argument.startswith("normal("):
        return stringify(eval(argument))

    elif argument.startswith("choice("):
        regex_choices = re.compile(r"\[(.+)\]")
        choices = regex_choices.findall(argument)[0]
        sanitized_choices = ','.join(map("'{0}'".format, choices.split(',')))

        arguments = eval(argument.replace(choices, sanitized_choices))
        if isinstance(arguments, np.string_):
            return [arguments]
        return arguments
    elif argument.startswith("["):
        regex_choices = re.compile(r"\[(.+)\]")
        choices = regex_choices.findall(argument)[0].split(',')
        return choices

    return argument.split()


def get_commands_from_arguments(arguments):
    commands = []
    for argvalues in product(*arguments):
        command = " ".join(argvalues)
        commands.append(command)

    return commands


def generate_name(arguments, max_length=255):
    # Creating the folder in 'LOGS_QSUB' where the results will be saved
    name = ''
    # TODO: Refactor name generator
    for argument in arguments:
        argname = argument[0][-30:] + ('' if len(argument) == 1 else ('-' + argument[-1][-30:]))
        argname = argname.split('/')[-1]  # Deal with path as parameter
        name += argname if name == '' else ('__' + argname)

    current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    name = current_time + ' ' + name[:max_length-len(current_time)-1]  # No more than 256 character
    return name


def create_job_folders(jobname):
    """Creates the folders where the logs and QSUB files will be saved."""
    path_logs = os.path.join(os.getcwd(), 'LOGS_QSUB')
    path_job_logs = os.path.join(path_logs, jobname)
    path_job_commands = os.path.join(path_job_logs, 'QSUB_commands')

    if not os.path.exists(path_job_commands):
        os.makedirs(path_job_commands)

    return path_job_logs, path_job_commands


def write_qsub_file(commands, qsub_filename, job_directory, queue, walltime, current_directory, use_cuda=False):
    """
    Example of a line for one job for QSUB:
        cd $SRC ; python -u trainAutoEnc2.py 10 80 sigmoid 0.1 vocKL_sarath_german True True > trainAutoEnc2.py-10-80-sigmoid-0.1-vocKL_sarath_german-True-True &
    """
    # Creating the file that will be launch by QSUB
    with open(qsub_filename, 'w') as qsub_file:
        qsub_file.write('#!/bin/bash\n')
        qsub_file.write('#PBS -q ' + queue + '\n')
        qsub_file.write('#PBS -l nodes=1:ppn=1\n')
        qsub_file.write('#PBS -V\n')
        qsub_file.write('#PBS -l walltime=' + walltime + '\n\n')

        if use_cuda:
            qsub_file.write('module load cuda\n')

        qsub_file.write('SRC_DIR_SMART_LAUNCHER=' + current_directory + '\n\n')

        command_template = 'cd $SRC_DIR_SMART_LAUNCHER; {0} &> "{1}" &\n'
        for command in commands:
            log_filename = os.path.join(job_directory, utils.generate_name_from_command(command))
            qsub_file.write(command_template.format(command, log_filename))

        qsub_file.write('\nwait\n')


if __name__ == "__main__":
    main()
