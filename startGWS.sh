#!/bin/bash

nohup celery -A graple-optimized.celery worker --loglevel=info &
nohup python graple-optimized.py &

