#!/bin/bash
docker compose run cli hz-cli cancel \
    -t=dev@hz  \
    temperature-monitor
