#!/bin/bash

/opt/hazelcast/bin/hz-cli submit -c=net.wrmay.jetdemo.$1 -t=dev@hz /project/temp-monitor/target/temp-monitor-1.0-SNAPSHOT.jar 
