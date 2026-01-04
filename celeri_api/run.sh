#!/bin/bash

#exec gunicorn main:app --bind 0.0.0.0:8000 -w 2 -k uvicorn.workers.UvicornWorker
exec gunicorn celeri_api.main:app --bind 0.0.0.0:8000 -w 2 -k uvicorn.workers.UvicornWorker
