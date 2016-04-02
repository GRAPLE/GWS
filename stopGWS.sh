#!/bin/bash

ps auxww | grep 'celery worker' | awk '{print $2}' | xargs kill
ps aux | grep 'graple-optimized.py' | awk '{print $2}' | xargs kill
