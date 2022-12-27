#!/bin/bash
docker compose run cli hz-cli submit \
    -c=net.wrmay.jetdemo.$1 \
    -t=dev@hz   \
    -n=temperature-monitor \
    /project/temp-monitor/target/temp-monitor-1.0-SNAPSHOT.jar
