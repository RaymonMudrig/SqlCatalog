#!/bin/bash

pip install fastapi uvicorn
uvicorn webapp:app --reload --port 8000
