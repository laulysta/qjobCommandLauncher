import os
import sys
import argparse
import time as t
from os.path import join as pjoin
from textwrap import dedent


import smartdispatch
from command_manager import CommandManager
import subprocess
from queue import Queue
from job_generator import job_generator_factory
from smartdispatch import get_available_queues
from smartdispatch import launch_jobs
from smartdispatch import utils

import logging
import smartdispatch

LOGS_FOLDERNAME = "SMART_DISPATCH_LOGS"
CLUSTER_NAME = utils.detect_cluster()
AVAILABLE_QUEUES = get_available_queues(CLUSTER_NAME)
LAUNCHER = utils.get_launcher(CLUSTER_NAME)

# Autoresume settings.
TIMEOUT_EXIT_CODE = 124
AUTORESUME_TRIGGER_AFTER = '$(($PBS_WALLTIME - 60))'  # By default, 60s before the maximum walltime.
AUTORESUME_WORKER_CALL_PREFIX = 'timeout -s TERM {trigger_after} '.format(trigger_after=AUTORESUME_TRIGGER_AFTER)
AUTORESUME_WORKER_CALL_SUFFIX = ' WORKER_PIDS+=" $!"'
AUTORESUME_PROLOG = 'WORKER_PIDS=""' 
AUTORESUME_EPILOG = """\
NEED_TO_RESUME=false
for WORKER_PID in $WORKER_PIDS; do
    wait "$WORKER_PID"
    RETURN_CODE=$?
    if [ $RETURN_CODE -eq {timeout_exit_code} ]; then
        NEED_TO_RESUME=true
    fi
done
if [ "$NEED_TO_RESUME" = true ]; then
    echo "Autoresuming using: {{launcher}} $PBS_FILENAME"
    sd-launch-pbs --launcher {{launcher}} $PBS_FILENAME {{path_job}}
fi
""".format(timeout_exit_code=TIMEOUT_EXIT_CODE)


def main(argv=None):
    # Necessary if we want 'logging.info' to appear in stderr.
    logging.root.setLevel(logging.INFO)

    args = parse_arguments(argv)
    path_smartdispatch_logs = pjoin(os.getcwd(), LOGS_FOLDERNAME)

    # Check if RESUME or LAUNCH mode
    if args.mode == "launch":
        if args.commandsFile is not None:
            # Commands are listed in a file.
            jobname = smartdispatch.generate_logfolder_name(os.path.basename(args.commandsFile.name), max_length=235)
            commands = smartdispatch.get_commands_from_file(args.commandsFile)
        else:
            # Command that needs to be parsed and unfolded.
            command = " ".join(args.commandAndOptions)
            jobname = smartdispatch.generate_name_from_command(command, max_length=235)
            commands = smartdispatch.unfold_command(command)

        commands = smartdispatch.replace_uid_tag(commands)
        nb_commands = len(commands)  # For print at the end

        if args.batchName:
            jobname = smartdispatch.generate_logfolder_name(utils.slugify(args.batchName), max_length=235)

    elif args.mode == "resume":
        jobname = args.batch_uid
        if os.path.isdir(jobname):
            # We assume `jobname` is `path_job` repo, we extract the real `jobname`.
            jobname = os.path.basename(os.path.abspath(jobname))

        if not os.path.isdir(pjoin(path_smartdispatch_logs, jobname)):
            raise LookupError("Batch UID ({0}) does not exist! Cannot resume.".format(jobname))
    else:
        raise ValueError("Unknown subcommand!")

    job_folders_paths = smartdispatch.get_job_folders(path_smartdispatch_logs, jobname)
    path_job, path_job_logs, path_job_commands = job_folders_paths

    # Keep a log of the command line in the job folder.
    command_line = " ".join(sys.argv)
    smartdispatch.log_command_line(path_job, command_line)

    command_manager = CommandManager(pjoin(path_job_commands, "commands.txt"))

    # If resume mode, reset running jobs
    if args.mode == "launch":
        command_manager.set_commands_to_run(commands)
    elif args.mode == "resume":
        # Verifying if there are failed commands
        failed_commands = command_manager.get_failed_commands()
        if len(failed_commands) > 0:
            FAILED_COMMAND_MESSAGE = dedent("""\
            {nb_failed} command(s) are in a failed state. They won't be resumed.
            Failed commands:
            {failed_commands}
            The actual errors can be found in the log folder under:
            {failed_commands_err_file}""")
            utils.print_boxed(FAILED_COMMAND_MESSAGE.format(
                nb_failed=len(failed_commands),
                failed_commands=''.join(failed_commands),
                failed_commands_err_file='\n'.join([utils.generate_uid_from_string(c[:-1]) + '.err' for c in failed_commands])
            ))

            if not utils.yes_no_prompt("Do you want to continue?", 'n'):
                exit()

        if args.expandPool is None:
            command_manager.reset_running_commands()

        nb_commands = command_manager.get_nb_commands_to_run()

        if args.expandPool is not None:
            args.pool = min(nb_commands, args.expandPool)

    # If no pool size is specified the number of commands is taken
    if args.pool is None:
        args.pool = command_manager.get_nb_commands_to_run()

    # Generating all the worker commands
    worker_script = pjoin(os.path.dirname(smartdispatch.__file__), 'workers', 'base_worker.py')
    worker_script_flags = ''
    if args.autoresume:
        worker_script_flags = '-r'

    worker_call_prefix = ''
    worker_call_suffix = ''
    if args.autoresume:
        worker_call_prefix = AUTORESUME_WORKER_CALL_PREFIX
        worker_call_suffix = AUTORESUME_WORKER_CALL_SUFFIX

    COMMAND_STRING = 'cd "{cwd}"; {worker_call_prefix}python2 {worker_script} {worker_script_flags} "{commands_file}" "{log_folder}" '\
                     '1>> "{log_folder}/worker/$PBS_JOBID\"\"_worker_{{ID}}.o" '\
                     '2>> "{log_folder}/worker/$PBS_JOBID\"\"_worker_{{ID}}.e" &'\
                     '{worker_call_suffix}'
    COMMAND_STRING = COMMAND_STRING.format(cwd=os.getcwd(), worker_call_prefix=worker_call_prefix, worker_script=worker_script,
                                           worker_script_flags=worker_script_flags, commands_file=command_manager._commands_filename,
                                           log_folder=path_job_logs, worker_call_suffix=worker_call_suffix)
    commands = [COMMAND_STRING.format(ID=i) for i in range(args.pool)]

    # TODO: use args.memPerNode instead of args.memPerNode
    queue = Queue(args.queueName, CLUSTER_NAME, args.walltime, args.coresPerNode, args.gpusPerNode, float('inf'), args.modules)

    # Change the default value of the gpusPerCommand depending on the value of 
    if args.gpusPerCommand is None:
        if queue.nb_gpus_per_node == 0:
            args.gpusPerCommand = 0
        else:
            args.gpusPerCommand = 1

    # Check that requested core number does not exceed node total
    if args.coresPerCommand > queue.nb_cores_per_node:
        sys.stderr.write("smart-dispatch: error: coresPerCommand exceeds nodes total: asked {req_cores} cores, nodes have {node_cores}\n"
                         .format(req_cores=args.coresPerCommand, node_cores=queue.nb_cores_per_node))
        sys.exit(2)

    # Check that requested gpu number does not exceed node total
    if args.gpusPerCommand > queue.nb_gpus_per_node:

        error_message = ("smart-dispatch: error: gpusPerCommand exceeds nodes total:" 
                         "asked {req_gpus} gpus, nodes have {node_gpus}. Make sure you have specified the correct queue.\n")

        sys.stderr.write(error_message.format(req_gpus=args.gpusPerCommand, node_gpus=queue.nb_gpus_per_node))
        sys.exit(2)


    command_params = {'nb_cores_per_command': args.coresPerCommand,
                      'nb_gpus_per_command': args.gpusPerCommand,
                      'mem_per_command': None  # args.memPerCommand
                      }

    prolog = []
    epilog = ['wait']
    if args.autoresume:
        prolog = [AUTORESUME_PROLOG]
        epilog = [AUTORESUME_EPILOG.format(launcher=LAUNCHER if args.launcher is None else args.launcher, path_job=path_job)]

    job_generator = job_generator_factory(queue, commands, prolog, epilog, command_params, CLUSTER_NAME, path_job)
    
    # generating default names per each jobs in each batch
    for pbs_id, pbs in enumerate(job_generator.pbs_list):
        proper_size_name = utils.jobname_generator(jobname, pbs_id)
        pbs.add_options(N=proper_size_name)
    
    if args.pbsFlags is not None:
        job_generator.add_pbs_flags(args.pbsFlags.split(' '))
    pbs_filenames = job_generator.write_pbs_files(path_job_commands)

    # Launch the jobs
    print "## {nb_commands} command(s) will be executed in {nb_jobs} job(s) ##".format(nb_commands=nb_commands, nb_jobs=len(pbs_filenames))
    print "Batch UID:\n{batch_uid}".format(batch_uid=jobname)
    if not args.doNotLaunch:
        
        try:
            launch_jobs(LAUNCHER if args.launcher is None else args.launcher, pbs_filenames, CLUSTER_NAME, path_job)
        except subprocess.CalledProcessError as e:

            cluster_advice = utils.get_advice(CLUSTER_NAME)

            error_message = ("smart-dispatch: error: The launcher wasn't"
                             " able the launch the job(s) properly. The"
                             " following error message was returned: \n\n{}"
                             "\n\nMaybe the pbs file(s) generated were"
                             " invalid. {}\n\n")


            sys.stderr.write(error_message.format(e.output, cluster_advice))
            sys.exit(2)

    print "\nLogs, command, and jobs id related to this batch will be in:\n {smartdispatch_folder}".format(smartdispatch_folder=path_job)


def parse_arguments(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queueName', required=True, help='Queue used (ex: qwork@mp2, qfat256@mp2, gpu_1)')
    parser.add_argument('-n', '--batchName', required=False, help='The name of the batch. Default: The commands launched.')
    parser.add_argument('-t', '--walltime', required=False, help='Set the estimated running time of your jobs using the DD:HH:MM:SS format. Note that they will be killed when this time limit is reached.')
    parser.add_argument('-L', '--launcher', choices=['qsub', 'msub'], required=False, help='Which launcher to use. Default: qsub')
    parser.add_argument('-C', '--coresPerNode', type=int, required=False, help='How many cores there are per node.')
    parser.add_argument('-G', '--gpusPerNode', type=int, required=False, help='How many gpus there are per node.')
    # parser.add_argument('-M', '--memPerNode', type=int, required=False, help='How much memory there are per node (in Gb).')

    parser.add_argument('-c', '--coresPerCommand', type=int, required=False, help='How many cores a command needs.', default=1)
    parser.add_argument('-g', '--gpusPerCommand', type=int, required=False, help='How many gpus a command needs. The value is 1 by default if GPUs are available on the specified queue, 0 otherwise.')
    # parser.add_argument('-m', '--memPerCommand', type=float, required=False, help='How much memory a command needs (in Gb).')
    parser.add_argument('-f', '--commandsFile', type=file, required=False, help='File containing commands to launch. Each command must be on a seperate line. (Replaces commandAndOptions)')

    parser.add_argument('-l', '--modules', type=str, required=False, help='List of additional modules to load.', nargs='+')
    parser.add_argument('-x', '--doNotLaunch', action='store_true', help='Generate all the files without launching the job.')
    parser.add_argument('-r', '--autoresume', action='store_true', help='Requeue the job when the running time hits the maximum walltime allowed on the cluster. Assumes that commands are resumable.')

    parser.add_argument('-p', '--pool', type=int, help="Number of workers that will be consuming commands. Default: Nb commands")
    parser.add_argument('--pbsFlags', type=str, help='ADVANCED USAGE: Allow to pass a space seperated list of PBS flags. Ex:--pbsFlags="-lfeature=k80 -t0-4"')
    subparsers = parser.add_subparsers(dest="mode")

    launch_parser = subparsers.add_parser('launch', help="Launch jobs.")
    launch_parser.add_argument("commandAndOptions", help="Options for the commands.", nargs=argparse.REMAINDER)

    resume_parser = subparsers.add_parser('resume', help="Resume jobs from batch UID.")
    resume_parser.add_argument('--expandPool', type=int, nargs='?', const=sys.maxsize, help='Add workers to the given batch. Default: # pending jobs.')
    resume_parser.add_argument("batch_uid", help="Batch UID of the jobs to resume.")

    args = parser.parse_args(argv)

    # Check for invalid arguments in
    if args.mode == "launch":
        if args.commandsFile is None and len(args.commandAndOptions) < 1:
            parser.error("You need to specify a command to launch.")
        if args.queueName not in AVAILABLE_QUEUES and ((args.coresPerNode is None and args.gpusPerNode is None) or args.walltime is None):
            parser.error("Unknown queue, --coresPerNode/--gpusPerNode and --walltime must be set.")
        if args.coresPerCommand < 1:
            parser.error("coresPerNode must be at least 1")
    
    return args


if __name__ == "__main__":
    main()
