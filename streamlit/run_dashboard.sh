#!/bin/bash
echo "Starting SpaceX Data Dashboard..."
cd "$(dirname "$0")"
streamlit run spacex_dashboard.py
