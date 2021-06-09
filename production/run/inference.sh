#!/bin/bash

PYTHONPATH=/usr/lib/python3/dist-packages
PYTHONPATH=$PYTHONPATH\:/usr/lib/python3.6/lib-dynload
export PYTHONPATH
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

# python3 ../source/wordfeature.py
python3 ../source/classify.py
