#!/bin/bash

PYTHONPATH=/usr/lib/python3/dist-packages
PYTHONPATH=$PYTHONPATH\:/usr/lib/python3.6/lib-dynload
export PYTHONPATH

python3 ../source/wordfeature.py
python3 ../source/classify.py
