#!/bin/bash
#PBS -q test
#PBS -V
#PBS -e "/home/negar/ccwtemp/smartdispatch/scripts/SMART_DISPATCH_LOGS/2016-11-25_17-31-37_homenegartestsh/logs/job/"$PBS_JOBID".err"
#PBS -o "/home/negar/ccwtemp/smartdispatch/scripts/SMART_DISPATCH_LOGS/2016-11-25_17-31-37_homenegartestsh/logs/job/"$PBS_JOBID".out"
#PBS -A jvb-000-ag
#PBS -l walltime=1:00
#PBS -l nodes=1:gpus=1

# Modules #
module load cuda/7.5.18

# Commands #
cd "/home/negar/ccwtemp/smartdispatch/scripts"; python2 /rap/jvb-000-aa/stack/conda/lib/python2.7/site-packages/smartdispatch/workers/base_worker.py "/home/negar/ccwtemp/smartdispatch/scripts/SMART_DISPATCH_LOGS/2016-11-25_17-31-37_homenegartestsh/commands/commands.txt" "/home/negar/ccwtemp/smartdispatch/scripts/SMART_DISPATCH_LOGS/2016-11-25_17-31-37_homenegartestsh/logs" 1>> "/home/negar/ccwtemp/smartdispatch/scripts/SMART_DISPATCH_LOGS/2016-11-25_17-31-37_homenegartestsh/logs/worker/$PBS_JOBID""_worker_0.o" 2>> "/home/negar/ccwtemp/smartdispatch/scripts/SMART_DISPATCH_LOGS/2016-11-25_17-31-37_homenegartestsh/logs/worker/$PBS_JOBID""_worker_0.e"  &

wait