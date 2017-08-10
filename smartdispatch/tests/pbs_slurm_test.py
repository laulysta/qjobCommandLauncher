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

def test_param(param_array, com, flag, string=pbs_string):
    for param in param_array:
        command = pbs_string.format(
            string.format(com.format(param))
        )
        with open("test.pbs", "w") as text_file:
            text_file.write(command)
        process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = process.communicate()
        assert_true("Submitted batch job" in stdout)
        job_id = stdout.split(" ")[-1].strip()

        time.sleep(0.25)
        process = Popen("squeue -u $USER -j {} -O {}".format(job_id, flag), stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = process.communicate()
        job_params = [c.strip() for c in stdout.split("\n")[1:] if c != '']
        # import ipdb; ipdb.set_trace()
        assert_true(all(p == param for p in job_params))

class TestSlurm(unittest.TestCase):

    def tearDown(self):
        process = Popen("rm *.out test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
        stdout, stderr = process.communicate()

    def test_priority(self):
        test_param(
            ['high', 'low'],
            "#SBATCH --qos={}",
            "qos",
            pbs_string
        )

    def test_gres(self):
        test_param(
            ['titanblack'],
            "#PBS -l naccelerators={}",
            "gres",
            pbs_string
        )

    def test_memory(self):
        test_param(
            ['2G', '4G'],
            "#PBS -l mem={}",
            "minmemory",
            pbs_string
        )

    def test_nb_cpus(self):
        test_param(
            ["1", "2"],
            "PBS -l ncpus={}",
            "numcpus",
            pbs_string
        )

    def test_constraint(self):
        test_param(
            ["gpu6gb", "gpu8gb"],
            "PBS -l proc={}",
            "feature",
            pbs_string
        )

#
# pbs_string2 = """\
# #!/usr/bin/env /bin/bash
#
# #PBS -N arrayJob
# #PBS -o arrayJob_%A_%a.out
# #PBS -t 1-5
# #PBS -l walltime=01:00:00
# #PBS -l naccelerators=1
# #PBS -l proc={cons}
# #PBS -M {mail}
# #SBATCH --qos {priority}
#
# ######################
# # Begin work section #
# ######################
#
# echo "My SLURM_ARRAY_JOB_ID:" $SLURM_ARRAY_JOB_ID
# echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID
# nvidia-smi
# """

if __name__ == '__main__':
    unittest.main()
