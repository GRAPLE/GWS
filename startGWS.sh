#!/bin/bash

nohup celery -A gws.celeryob worker --loglevel=info 1> celery.out 2> celery.err &
nohup python ems.py 1> ems.out 2> ems.err &
nohup python gws.py 1> uwsgi.out 2> uwsgi.err &
