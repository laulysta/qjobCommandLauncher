import time
import unittest


from subprocess import Popen, PIPE
from nose.tools import assert_equal, assert_true

pbs_string = """\
#!/usr/bin/env /bin/bash

#PBS -N arrayJob
#PBS -o arrayJob_%A_%a.out
#PBS -t 1-2
#PBS -l walltime=01:00:00
#PBS -l naccelerators=1
{}

######################
# Begin work section #
######################

echo "My SLURM_ARRAY_JOB_ID:" $SLURM_ARRAY_JOB_ID
echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID
nvidia-smi
"""

sbatch_string = """\
#!/usr/bin/env -i /bin/zsh

#SBATCH --job-name=arrayJob
#SBATCH --output=arrayJob_%A_%a.out
#SBATCH --array=0-5
#SBATCH --time=01:00:00
#SBATCH --gres=gpu
#SBATCH --constraint=gpu6gb

######################
# Begin work section #
######################

echo "My SLURM_ARRAY_JOB_ID:" $SLURM_ARRAY_JOB_ID
echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID
nvidia-smi
"""


class TestSlurm(unittest.TestCase):

    def test_priority(self):
        priorities = ['high', 'low']
        for priority in priorities:
            string = pbs_string.format(
                "#SBATCH --qos={priority}".format(priority=priority)
            )
            with open("test.pbs", "w") as text_file:
                text_file.write(string)
            process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = process.communicate()
            assert_true("Submitted batch job" in stdout)
            job_id = stdout.split(" ")[-1]

            time.sleep(0.25)
            process = Popen("squeue -u $USER -j {} -O qos".format(job_id), stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = process.communicate()
            job_priorities = [prio.strip() for prio in stdout.split("\n")[1:] if prio != '']
            assert_true(all(pri == priority for pri in job_priorities))

    def test_gres(self):
        gress = ['gpu', 'gpu:titanblack']
        for gres in gress:
            string = pbs_string.format(
                "#PBS -l gpu={gres}".format(gres=gres)
            )
            with open("test.pbs", "w") as text_file:
                text_file.write(string)
            process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = process.communicate()
            assert_true("Submitted batch job" in stdout)
            job_id = stdout.split(" ")[-1]

            time.sleep(0.25)
            process = Popen("squeue -u $USER -j {} -O gres".format(job_id), stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = process.communicate()
            job_gres = [gre.strip() for gre in stdout.split("\n")[1:] if gre != '']
            assert_true(all(gpu == gres for gpu in job_gres))

    def test_memory(self):
        mems = ['2G', '4G']
        for mem in mems:
            string = pbs_string.format(
                "#PBS -l mem_free={memory}".format(memory=mem)
            )
            with open("test.pbs", "w") as text_file:
                text_file.write(string)
            process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = process.communicate()
            assert_true("Submitted batch job" in stdout)
            job_id = stdout.split(" ")[-1]

            time.sleep(0.25)
            process = Popen("squeue -u $USER -j {} -O minmemory".format(job_id), stdout=PIPE, stderr=PIPE, shell=True)
            stdout, stderr = process.communicate()
            job_mems = [m.strip() for m in stdout.split("\n")[1:] if m != '']
            assert_true(all(me == mem for me in job_mems))

    # def test_pbs_slurm(self):
    #     priorities = ['unkillable', 'high', 'low']
    #     # doesn't test 12 and 24 to avoid killing jobs just for testing
    #     constraints = ['gpulowmem', 'gpu6gb', 'gpu8gb']
    #     mem = ['2G', '4G', '6G']
    #     mail = 'adrien.alitaiga@gmail.com'
    #     # gpus = ['titanblack']
    #
    #     for priority in priorities:
    #         for cons in constraints:
    #             for m in mem:
    #                 string = pbs_string.format(**{
    #                     'cons': cons,
    #                     'mail': mail,
    #                     'priority': priority
    #                 })
    #                 with open("test.pbs", "w") as text_file:
    #                     text_file.write(string)
    #                 process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
    #                 time.sleep(1)
    #
    #                 stdout, stderr = process.communicate()
    #                 print stdout
    #                 assert_true("Submitted batch job" in stdout)


pbs_string2 = """\
#!/usr/bin/env /bin/bash

#PBS -N arrayJob
#PBS -o arrayJob_%A_%a.out
#PBS -t 1-5
#PBS -l walltime=01:00:00
#PBS -l naccelerators=1
#PBS -l proc={cons}
#PBS -M {mail}
#SBATCH --qos {priority}

######################
# Begin work section #
######################

echo "My SLURM_ARRAY_JOB_ID:" $SLURM_ARRAY_JOB_ID
echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID
nvidia-smi
"""

if __name__ == '__main__':
    unittest.main()
