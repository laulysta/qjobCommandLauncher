from glob import glob
import os
import time
import unittest
from subprocess import Popen, PIPE

from smartdispatch.utils import get_slurm_cluster_name

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

# Checking which cluster is running the tests first
cluster = get_slurm_cluster_name()
to_skip = cluster in ['graham', 'cedar']
message = "Test does not run on cluster {}".format(cluster)

class TestSlurm(unittest.TestCase):

    def tearDown(self):
        for file_name in (glob('*.out') + ["test.pbs"]):
            os.remove(file_name)

    def _test_param(self, param_array, command_template, flag, string=pbs_string, output_array=None):
        output_array = output_array or param_array
        for param, output in zip(param_array, output_array):
            param_command = pbs_string.format(
                string.format(command_template.format(param))
            )
            with open("test.pbs", "w") as text_file:
                text_file.write(param_command)
            process = Popen("sbatch test.pbs", stdout=PIPE, stderr=PIPE, shell=True)
            stdout, _ = process.communicate()
            stdout = stdout.decode()
            self.assertIn("Submitted batch job", stdout)
            job_id = stdout.split(" ")[-1].strip()

            time.sleep(0.25)
            process = Popen("squeue -u $USER -j {} -O {}".format(job_id, flag), stdout=PIPE, stderr=PIPE, shell=True)
            stdout, _ = process.communicate()
            job_params = [c.strip() for c in stdout.decode().split("\n")[1:] if c != '']
            self.assertSequenceEqual(job_params, [output for _ in range(len(job_params))])

    @unittest.skipIf(to_skip, message)
    def test_priority(self):
        self._test_param(
            ['high', 'low'],
            "#SBATCH --qos={}",
            "qos",
            pbs_string
        )

    def test_gres(self):
        self._test_param(
            ["1", "2"],
            "#PBS -l naccelerators={}",
            "gres",
            pbs_string,
            ["gpu:1", "gpu:2"]
        )

    def test_memory(self):
        self._test_param(
            ["2G", "4G"],
            "#PBS -l mem={}",
            "minmemory",
            pbs_string
        )

    def test_nb_cpus(self):
        self._test_param(
            ["2", "3"],
            "#PBS -l ncpus={}",
            "mincpus",
            pbs_string
        )

    @unittest.skipIf(to_skip, message)
    def test_constraint(self):
        self._test_param(
            ["gpu6gb", "gpu8gb"],
            "#PBS -l proc={}",
            "feature",
            pbs_string
        )

if __name__ == '__main__':
    unittest.main()
