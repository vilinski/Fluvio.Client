#!/bin/bash

# Delete all topics
for topic in $(fluvio topic list | tail -n +2 | awk '{print $1}'); do
    fluvio topic delete "$topic"
done