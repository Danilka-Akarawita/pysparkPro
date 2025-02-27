#!/bin/bash

python code/src/run.py process_data \
    --cfg code/config/cfg.yaml \
    --dataset news \
    --dirout ztmp/data

python code/src/run.py process_data_all \
    --cfg code/config/cfg.yaml \
    --dataset news \
    --dirout ztmp/data
