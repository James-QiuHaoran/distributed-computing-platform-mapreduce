#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Illegal number of inputs. Usage: ./start_master.sh id mode"
else
    # server id from 1 to 10
    id=$1

    # mode for starting a server - 1 for bootstrap mode & 2 for normal node
    mode=$2

    cd ../classes
    java MapReduceMaster $id $mode
fi
