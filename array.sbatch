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
