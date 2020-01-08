#!/bin/bash

# create folders for later usage
[ ! -d "../classes" ] && mkdir ../classes
[ ! -d "../logs" ] && mkdir ../logs
[ ! -d "../sdfs_dir" ] && mkdir ../sdfs_dir

# Node Class
javac ../src/Node.java -d ../classes/ -cp ../classes/
if [ $? = "0" ]; then
  echo "Node class was successfully bulit."
else
  echo "Compilation error occurred when building Node class!"
  exit 1
fi

# SDFSFile Class
javac ../src/SDFSFile.java -d ../classes/ -cp ../classes/
if [ $? = "0" ]; then
  echo "SDFSFile class was successfully bulit."
else
  echo "Compilation error occurred when building SDFSFile class!"
  exit 1
fi

# Task Class
javac ../src/Task.java -d ../classes/ -cp ../classes/
if [ $? = "0" ]; then
  echo "Task class was successfully bulit."
else
  echo "Compilation error occurred when building Task class!"
  exit 1
fi

# Job Class
javac ../src/Job.java -d ../classes/ -cp ../classes/
if [ $? = "0" ]; then
  echo "Job class was successfully bulit."
else
  echo "Compilation error occurred when building Job class!"
  exit 1
fi

# MembershipListMaintainer Class
javac ../src/MembershipListMaintainer.java -d ../classes/ -cp ../classes/
if [ $? = "0" ]; then
  echo "MembershipListMaintainer class was successfully built."
else
  echo "Compilation error occurred when building MembershipListMaintainer class!"
  exit 1
fi

# SDFSServer Class
javac ../src/SDFSServer.java -d ../classes/ -cp ../classes
if [ $? = "0" ]; then
  echo "SDFSServer class was successfully built."
else
  echo "Compilation error occurred when building SDFSServer class!"
  exit 1
fi

# MapReduceMaster Class
javac ../src/MapReduceMaster.java -d ../classes/ -cp ../classes
if [ $? = "0" ]; then
  echo "MapReduceMaster class was successfully built."
else
  echo "Compilation error occurred when building MapReduceMaster class!"
  exit 1
fi

# MapReduceWorker Class
javac ../src/MapReduceWorker.java -d ../classes/ -cp ../classes
if [ $? = "0" ]; then
  echo "MapReduceWorker class was successfully built."
else
  echo "Compilation error occurred when building MapReduceWorker class!"
  exit 1
fi

# Usage
echo ""
echo "Compilation completed."
echo "To start a MapReduce master, execute \"./start_master.sh id mode\"."
echo "To start a MapReduce worker, execute \"./start_worker.sh id mode\"."
echo ""
