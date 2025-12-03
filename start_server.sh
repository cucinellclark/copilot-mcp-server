#!/bin/bash
source copilot_mcp_env/bin/activate

# Run the server
PORT=8052 python3 http_server.py
