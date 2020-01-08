## Introduction

This is a parallel cloud computing framework called MapleJuice, which bears similarities with MapReduce/Hadoop.
This distributed computing platform consists of services like an underlying distributed file system and a reliable membership protocol.
This is extended from a course project (CS 425 at UIUC) and is awarded the best implementation in Java.
You can only use this for reference if you are also implementing MapleJuice for CS 425.

## Description

The interface of MapleJuice consists of two phases of computation - Maple and Juice.
Each phase is divided into tasks running parallelly on servers in the cluster.
Two phases are separated by a barriers, meaning that Juice can not be started when Maple is still running.

The Maple function processes 10 input lines (from a file) simultaneously at a time, while the traditional Map function processed only one input line
at a time.
Beyond this distinction, the MapleJuice paradigm is very similar to the MapReduce paradigm. 

The MapleJuice cluster has N server machines where one of them is the master server and all other N-1 machines are worker servers.

### MapReduce Master

The master is responsible for all critical functionalities related to coordination activity: 

- receiving Maple and Juice commands,
- scheduling appropriate Maple/Juice tasks, 
- allocating keys to Juice (Reduce) tasks, 
- tracking progress/completion of tasks, and 
- dealing with failures of the other (worker) servers.

MapleJuice cluster only accepts one command (maple or juice)
at a time, i.e., while the cluster is processing maple (or juice) tasks, no other juice (or
maple) tasks can be submitted. However, a sequence of jobs can be submitted (they will
be queued to be executed one at a time).

When a worker fails, the master will reschedule the task quickly so that the job can still complete. 
When a worker rejoins the system, the master will consider it for new tasks. 
Worker failures do not result in incorrect output.

### MapReduce Worker

The Worker will retrieve needed data file from the SDFS first.
Then each worker will execute the application passed by the Master on the input data.
For a key `K`, all `(K, any_value)` pairs output by a worker will be appended to the file `sdfs_intermediate_filename_prefix_K`
(with `K` appropriately modified to remove any special characters). 
Therefore, the output of the Maple phase (not task) is a series of SDFS files, one per key.
This is another difference from MapReduce.

Each Juice task is responsible for a portion of the keys – each key is allotted to exactly one
Juice task (this is done by the Master server). The Juice task fetches the relevant SDFS files
(`sdfs_intermediate_filename_prefix_K`), processes the input lines in them, and
appends all its output to `sdfs_dest_filename`. 

### Terminal (Commands)

For Master:
- `join` - join the group;
- `leave` - leave the group;
- `list` - list all the members in the group (active and idle) and its neighbors;
- `info` - print current server id, neighbors, on-time, etc.;
- `exit` - exit the command line environment (make sure the the node's status is idle/leave);
- `put [local_file_name] [sdfs_file_name]`: insert or update file;
- `get [sdfs_file_name] [local_file_name]`: fetch file to local;
- [new] `get-dir [sdfs_dir] [local_dir]`: fetch all files under `sdfs_dir` to `local_dir`;
- `delete [sdfs_file_name]`: delete a file;
- `ls [sdfs_file_name]`: list all the machines where the file is currently being stored;
- `store`: list all files which are currently being stored at this machine;
- [new] `maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>`;
    - `maple_exe`: user-specified executable (stored on SDFS) that takes as input one file and outputs a series of `(key, value)` pairs
    - `num_maples`: the number of Maple tasks
    - `sdfs_intermediate_filename_prefix`: prefix of intermediate files generated in Maple phase
    - `sdfs_src_directory`: the location of the input files (directory) on SDFS
- [new] `juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}`
    - `juice_exe`: user-specified executable (stored on SDFS) that takes as input multiple `(key, value)` input lines, processes groups of `(key, any_values)` input lines together (sharing the same key, just like Reduce), and outputs `(key, value)` pairs
    - `num_juices`: the number of Juice tasks
    - `sdfs_intermediate_filename_prefix`: prefix of intermediate files generated in Maple phase which are used as input in Juice phase
    - `sdfs_dest_filename`: output file name from the Juice phase
    - `delete_input`: 1 for deleting intermediate files after MapleJuice is done, 0 for otherwise

For Worker:
- `join` - join the group;
- `leave` - leave the group;
- `list` - list all the members in the group (active and idle) and its neighbors;
- `info` - print current server id, neighbors, on-time, etc.;
- `exit` - exit the command line environment (make sure the the node's status is idle/leave);
- `put [local_file_name] [sdfs_file_name]`: insert or update file;
- `get [sdfs_file_name] [local_file_name]`: fetch file to local;
- [new] `get-dir [sdfs_dir] [local_dir]`: fetch all files under `sdfs_dir` to `local_dir`;
- `delete [sdfs_file_name]`: delete a file;
- `ls [sdfs_file_name]`: list all the machines where the file is currently being stored;
- `store`: list all files which are currently being stored at this machine;
- [new] `maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>` - redirect the request to the Master;
- [new] `juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}` - redirect the request to the Master;

## How to Build

```
cd scripts
./build.sh
```

## How to Run

You should firstly start 6 servers (VM1-VM6) in the bootstrap mode (1). After that, every node should be started in the normal mode (2).

```
./start_master.sh id mode
./start_worker.sh id mode
```

- `id`: id of the virtual machine - from 1 to 10;
- `mode`: 1 for bootstrap mode & 2 for normal mode;

Before you run, you may want to clean up the log files & previous SDFS file directory and kill those processes using certain port numbers.

```
./clean.sh
```

## How to Execute a MapleJuice Job

1. Write your own application in MapleJuice paradigm (refer to the two applications provided in `applications/`).
2. Upload the Maple and Juice executables to the SDFS (e.g. `put path/to/application maple_exe`).
3. Upload the input files to the SDFS under the same directory (e.g. `put path/to/input/files/file1 input_dir/file1`).
4. Execute Maple tasks (e.g. `maple maple_exe 10 tmp_tasks_ input_dir`).
5. Execute Juice tasks (e.g. `juice juice_exe 10 tmp_tasks_ output_file 1`).
6. Check the result in `output_file`!

## Applications

MapleJuice platform supports Python scripts only. A Maple application need to deal with multiple lines as the input at a time. The format is `line\nline\nline`. The output should be `key,value\nkey,value`.
A Juice application need to deal with a key and a string of values as the input. The format is `key value1,value2,value3`. The output should be `key,value`.

There are two application examples included in the `applications/` directory. One is the word count application and the other is reverse web-link graph application.
To run the two applications on this distributed computing platform (MapleJuice), you need to follow the above instructions and treat `wc-maple/juice.py` and `rwlg-maple/juice.py` as executables.

## Assumptions

- At most 3 node failures (simultaneously);
- Only accepts one command (maple or juice) at a time;
- Filename cannot contain "\_" and "-";
- The number of input files should be larger than the number of Maple tasks;
- The number of keys (intermediate files) should be larger than the number of Juice tasks;
- Master node is fault-free;