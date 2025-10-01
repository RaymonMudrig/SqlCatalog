#!/bin/bash

# pip install fastapi uvicorn
# uvicorn webapp:app --reload --port 8000

# export CHAT_API_BASE=http://127.0.0.1:1234/v1
# export CHAT_MODEL=qwen2.5-32b-instruct-mlx
uvicorn webapp:app --reload --port 8000
# open http://127.0.0.1:8000/
# select "Agent (natural answer)" and ask:
#   Which procedure access table 'Order'?
#   what does RT_Order store?