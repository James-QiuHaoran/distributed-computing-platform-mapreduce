#!/bin/bash

# delete all log files
rm -rf ../logs/*

# delete the SDFS file directory
rm -rf ../sdfs_dir/*

# kill all processes using the port 3001, 3002, 5001, 5002, 6001
pid_3001="$(lsof -i :3001 | tail -1 | cut -d$' ' -f5)"
if [ ! -z "$pid_3001" ]
then
    kill -9 "$pid_3001"
fi

pid_3002="$(lsof -i :3002 | tail -1 | cut -d$' ' -f5)"

if [ ! -z "$pid_3002" ]
then
    kill -9 "$pid_3002"
fi

pid_5001="$(lsof -i :5001 | tail -1 | cut -d$' ' -f5)"

if [ ! -z "$pid_5001" ]
then
    kill -9 "$pid_5001"
fi

pid_5002="$(lsof -i :5002 | tail -1 | cut -d$' ' -f5)"

if [ ! -z "$pid_5002" ]
then
    kill -9 "$pid_5002"
fi

pid_6001="$(lsof -i :6001 | tail -1 | cut -d$' ' -f5)"

if [ ! -z "$pid_6001" ]
then
    kill -9 "$pid_6001"
fi

echo "Clean-up for zombie processes, log files, and sdfs directory done!"