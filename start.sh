#!/bin/sh
gunicorn -b 0.0.0.0 -w 4 -t 60 "jetlag:app"
