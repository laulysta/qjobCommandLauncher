from glob import glob
import os
import time
import unittest

from subprocess import Popen, PIPE

pbs_string = """\
#!/usr/bin/env /bin/bash

#PBS -N arrayJob
#PBS -o arrayJob_%A_%a.out
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
#SBATCH --time=01:00:00
#SBATCH --gres=gpu
#SBATCH --constraint=gpu6gb
{}

######################
# Begin work section #
######################

echo "My SLURM_ARRAY_JOB_ID:" $SLURM_ARRAY_JOB_ID
echo "My SLURM_ARRAY_TASK_ID: " $SLURM_ARRAY_TASK_ID
nvidia-smi
"""


class TestSlurm(unittest.TestCase):

    def tearDown(self):
        for file_name in (glob('*.out') + ["test.pbs"]):
            os.remove(file_name)

    def _test_param(self, param_array, com, flag, string=pbs_string):
        for param in param_array:
            command = pbs_string.format(
                string.format(com.format(param))
            )
            with open("test.pbs", "w") as text_file:
                text_file.write(command)
            process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
            stdout, _ = process.communicate()
            stdout = stdout.decode()
            print(stdout)
            self.assertIn("Submitted batch job", stdout)
            job_id = stdout.split(" ")[-1].strip()

            time.sleep(0.25)
            process = Popen("squeue -u $USER -j {} -O {}".format(job_id, flag), stdout=PIPE, stderr=PIPE, shell=True)
            stdout, _ = process.communicate()
            job_params = [c.strip() for c in stdout.decode().split("\n")[1:] if c != '']
            # import ipdb; ipdb.set_trace()
            self.assertSequenceEqual(job_params, [param for _ in range(len(job_params))])

    def test_priority(self):
        self._test_param(
            ['high', 'low'],
            "#SBATCH --qos={}",
            "qos",
            pbs_string
        )

    def test_gres(self):
        self._test_param(
            ['k80'],
            "#PBS -l naccelerators={}",
            "gres",
            pbs_string
        )

    def test_memory(self):
        self._test_param(
            ['2G', '4G'],
            "#PBS -l mem={}",
            "minmemory",
            pbs_string
        )

    def test_nb_cpus(self):
        self._test_param(
            ["2", "3"],
            "#PBS -l mppdepth={}",
            # "#SBATCH --cpus-per-task={}",
            "numcpus",
            pbs_string
        )

    def test_constraint(self):
        self._test_param(
            ["gpu6gb", "gpu8gb"],
            "#PBS -l proc={}",
            "feature",
            pbs_string
        )

if __name__ == '__main__':
    unittest.main()
