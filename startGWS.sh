#!/bin/bash

nohup celery -A gws.celery worker --loglevel=info &
nohup python gws.py &
