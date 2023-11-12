#!/bin/bash

./stop_bridge_agent.sh

sleep 1.25

nohup python3 -u ./bridge_agent.py >> logs/bridge_$(date '+%Y%m%d-%H%M%S').log 2>&1 &

