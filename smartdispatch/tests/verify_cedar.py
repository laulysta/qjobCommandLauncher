import os
import sys

from verify_slurm_cluster import VerifySlurmCluster, set_defaults


class VerifyCedarCluster(VerifySlurmCluster):

    WALLTIME = 60
    CORES_PER_NODE = 24
    GPUS_PER_NODE = 4

    def get_arguments(self, **kwargs):

        kwargs = super(VerifyCedarCluster, self).get_arguments(**kwargs)

        if kwargs["gpusPerCommand"] == 0:
            account = os.environ.get("CPU_SLURM_ACCOUNT")
        else:
            account = os.environ.get("GPU_SLURM_ACCOUNT")

        if "sbatchFlags" not in kwargs or len(kwargs["sbatchFlags"]) == 0:
            kwargs["sbatchFlags"] = "--account=" + account
        else:
            kwargs["sbatchFlags"] += " --account=" + account

        return kwargs


if __name__ == "__main__":
    verifications = filter(lambda o: not o.startswith("--"), sys.argv[1:])
    VerifyCedarCluster(debug="--debug" in sys.argv[1:],
                       no_fork="--no-fork" in sys.argv[1:]).run_verifications(
        filtered_by=verifications)
