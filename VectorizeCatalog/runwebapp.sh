#!/bin/bash

# pip install fastapi uvicorn
# Use explicit path to webapp.py to avoid conflict with webapp/ directory

python3 -m uvicorn webapp:app --reload --port 8000 --host 0.0.0.0
