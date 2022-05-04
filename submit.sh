#!/bin/bash

/opt/hazelcast/bin/hz-cli submit -c=net.wrmay.jetdemo.$1 -t=dev@hz /project/event-processor/target/event-processor-1.0-SNAPSHOT.jar 