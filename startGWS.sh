#!/bin/bash

nohup celery -A gws.celeryob worker --loglevel=info > celery.out &
nohup python gws.py > uwsgi.out &
