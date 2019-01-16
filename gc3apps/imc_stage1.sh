#!/bin/bash

echo "[`date`]: Start"

# Activate conda environment
source /opt/anaconda2/bin/activate

# Activate imc environment
conda activate imc

# run TP IMC processing
tp_imc_script=$1
raw_data_location=$2
metadata_location=$3
derived_location=$4
config_file=$5

python $tp_imc_script $raw_data_location $metadata_location $derived_location $config_file
RET=$?

echo "[`date`]: Done with code $RET"
exit $RET
