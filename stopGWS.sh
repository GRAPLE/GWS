#!/bin/bash

pgrep -f 'celeryob worker' | xargs kill
pgrep -f 'gws.py' | xargs kill
pgrep -f 'ems.py' | xargs kill
ps -ef | grep 'ems.py'
