#!/bin/sh
gunicorn -w 4 -t 60 "jetlag:app"
