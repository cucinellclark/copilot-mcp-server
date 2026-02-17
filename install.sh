#!/bin/bash

python3 -m venv copilot_mcp_env

source copilot_mcp_env/bin/activate

git clone git@github.com:cucinellclark/rag_api.git
cd rap_api
pip install -r requirements.txt

cd ..
pip install -r requirements.txt
