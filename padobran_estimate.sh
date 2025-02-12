#!/bin/bash

#PBS -N Season
#PBS -l ncpus=1
#PBS -l mem=4GB
#PBS -J 1-17058
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image_season.sif padobran_estimate.R
