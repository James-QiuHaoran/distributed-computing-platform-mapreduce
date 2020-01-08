import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class MapReduceMaster {
    // SDFS file server
    private SDFSServer sdfsServer;

    // UDP port for sending messages to the MapReduce workers
    private static final int port = 6001;

    // UDP socket programming
    private DatagramSocket socket;

    // masters
    private ArrayList<Integer> masterList;

    // job queue
    private Queue<Job> jobQueue;

    // for current job
    private int numFinishedTasks = 0;
    private ArrayList<Task> task_list;
    private List<Integer> unprocessedCompletedTasks;
    private HashMap<String, String> sdfsToLocalMapping;

    // logging
    private void log(String content) {
        sdfsServer.getMembershipListMaintainer().log(content);
    }

    // send via UDP - return 0 if no error when sending, -1 otherwise
    private int sendMSG(DatagramSocket socket, String msg, String dest_ip, String message_type) {
        byte[] buf = msg.getBytes();
        DatagramPacket packet;
        try {
            packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(dest_ip), port);
        } catch (UnknownHostException e) {
            log("[ERROR] - error creating UDP packet - unknown host [" + dest_ip + "]");
            e.printStackTrace();
            return -1;
        }
        try {
            socket.send(packet);
        } catch (IOException e) {
            log("[ERROR] - error sending UDP packet [" + dest_ip + "]");
            e.printStackTrace();
        }
        log("[MESSAGE] - message sent to " + dest_ip + ":" + port + " - " + message_type);
        return 0;
    }

    // distribute map tasks
    private void map(String maple_exe, int num_maples, String sdfs_intermediate_filename_prefix, String sdfs_src_directory) {
        // calculate payload for each maple task
        ArrayList<String> input_file_names = this.sdfsServer.get_dir_meta(sdfs_src_directory);
        int num_input_files = input_file_names.size();
        int payload = num_input_files / num_maples;

        // generate tasks
        task_list = new ArrayList<Task>();
        ArrayList<Task> active_task_list = new ArrayList<Task>();
        ArrayList<Task> inactive_task_list = new ArrayList<Task>();
        ArrayList<Integer> workersInUse = new ArrayList<Integer>();
        HashMap<Integer, Task> workerTaskMapping = new HashMap<Integer, Task>();
        ArrayList<Integer> activeMemberIDs = this.sdfsServer.getMembershipListMaintainer().getAllActiveMembers();
        activeMemberIDs.removeAll(this.masterList);
        int fileCounter = 0;
        for (int i = 0; i < num_maples; i++) {
            ArrayList<String> inputFiles = new ArrayList<String>();
            if (i < num_input_files % num_maples) {
                for (int j = 0; j < payload + 1; j++) {
                    inputFiles.add(input_file_names.get(fileCounter + j));
                }
                fileCounter += payload + 1;
            } else {
                for (int j = 0; j < payload; j++) {
                    inputFiles.add(input_file_names.get(fileCounter + j));
                }
                fileCounter += payload;
            }
            if (workersInUse.size() < activeMemberIDs.size()) {
                Task task = new Task("Map", i + 1, activeMemberIDs.get(workersInUse.size()), inputFiles, 0);
                task_list.add(task);
                active_task_list.add(task);
                workerTaskMapping.put(activeMemberIDs.get(workersInUse.size()), task);
                workersInUse.add(activeMemberIDs.get(workersInUse.size()));
            } else {
                Task task = new Task("Map", i + 1, -1, inputFiles, 0);
                task_list.add(task);
                inactive_task_list.add(task);
            }
        }

        // print task info and distribute active tasks to the workers
        for (Task t : task_list) {
            t.printInfo();
            if (t.getNodeID() == -1) {
                continue;
            }
            String msg = "maple-payload_" + this.sdfsServer.getIP() + "_" + t.getTaskID() + "_" + maple_exe + "_" + sdfs_src_directory + "_";
            for (String f : t.getInputFiles()) {
                msg += f + "|";
            }
            sendMSG(this.socket, msg, this.sdfsServer.getMembershipListMaintainer().getMembership_list().get(t.getNodeID()).getIp(), "distribute-maple-task");
        }

        // tracking the progress
        numFinishedTasks = 0;
        unprocessedCompletedTasks = new CopyOnWriteArrayList<Integer>();
        sdfsToLocalMapping = new HashMap<String, String>();
        while (numFinishedTasks < num_maples) {
            // wait for 1 second
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // check if there's a new finished task -> if yes: store output from the worker to the intermediate files on SDFS
            ArrayList<Integer> tmp_processed = new ArrayList<Integer>();
            for (Integer i : unprocessedCompletedTasks) {
                // fetch output file from the worker
                Task t = task_list.get(i - 1);
                System.out.println("Processing output file " + t.getOutputFile() + " from Node #" + (t.getNodeID() + 1) + "...");
                String currentDirectory = System.getProperty("user.dir");
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                String fileName = "maple_task_" + timestamp.getTime() + "_" + t.getTaskID() + ".output";
                String localDir = currentDirectory.substring(0, currentDirectory.length() - 7);
                String localPath = localDir + "sdfs_dir/" + fileName;
                this.sdfsServer.send_request(this.sdfsServer.getMembershipListMaintainer().getMembership_list().get(t.getNodeID()).getIp(), t.getOutputFile(), localPath);
                workersInUse.remove(Integer.valueOf(t.getNodeID()));

                // read from the output file
                Scanner input = new Scanner(System.in);
                File file = new File(localPath);
                try {
                    input = new Scanner(file);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                HashMap<String, ArrayList<String>> keyValuePairs = new HashMap<String, ArrayList<String>>();
                while (input.hasNextLine()) {
                    String line = input.nextLine();
                    String key = line.split(",")[0];
                    String value = line.split(",")[1];
                    if (keyValuePairs.containsKey(key)) {
                        keyValuePairs.get(key).add(value);
                    } else {
                        ArrayList<String> listOfValues = new ArrayList<String>();
                        listOfValues.add(value);
                        keyValuePairs.put(key, listOfValues);
                    }
                }

                // append key-value pairs to the corresponding intermediate files
                if (!keyValuePairs.isEmpty()) {
                    FileWriter fw = null;
                    for (String key : keyValuePairs.keySet()) {
                        localPath = localDir + "sdfs_dir/" + sdfs_intermediate_filename_prefix + key;
                        File f = new File(localPath);
                        if (!sdfsToLocalMapping.containsKey(sdfs_intermediate_filename_prefix + "/" + key)) {
                            sdfsToLocalMapping.put(sdfs_intermediate_filename_prefix + "/" + key, localPath);
                            try {
                                f.createNewFile();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            fw = new FileWriter(f, true);
                        } catch (IOException e) {
                            this.sdfsServer.getMembershipListMaintainer().log("[ERROR] - File Writer creation failed!");
                            e.printStackTrace();
                        }
                        BufferedWriter bw = null;
                        assert fw != null;
                        bw = new BufferedWriter(fw);
                        for (String val : keyValuePairs.get(key)) {
                            try {
                                bw.write(val);
                                bw.newLine();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        try {
                            bw.close();
                            fw.close();
                        } catch (IOException e) {
                            this.sdfsServer.getMembershipListMaintainer().log("[ERROR] - File Writer / Buffered Writer closing failed!");
                            e.printStackTrace();
                        }
                    }
                }
                input.close();

                // delete files and commit
                this.sdfsServer.delete(t.getOutputFile());
                t.setCommitted(true);
                MapReduceMaster.this.numFinishedTasks += 1;
                tmp_processed.add(i);
                System.out.println("Processing output file " + t.getOutputFile() + " from Node #" + (t.getNodeID() + 1) + " completed.");
            }
            unprocessedCompletedTasks.removeAll(tmp_processed);

            // remove idle workers and find new available worker(s)
            ArrayList<Integer> newActiveIDs = this.sdfsServer.getMembershipListMaintainer().getAllActiveMembers();
            newActiveIDs.removeAll(masterList);
            ArrayList<Integer> tmp_workers = new ArrayList<Integer>();
            for (Integer i : workersInUse) {
                if (!newActiveIDs.contains(i)) {
                    // this is an idle worker
                    tmp_workers.add(i);

                    // put that task to inactive
                    workerTaskMapping.get(i).setNodeID(-1);
                    workerTaskMapping.get(i).setProgress(-1);
                    Task t = workerTaskMapping.get(i);
                    workerTaskMapping.remove(i);
                    active_task_list.remove(t);
                    inactive_task_list.add(t);
                }
            }
            workersInUse.removeAll(tmp_workers);
            ArrayList<Integer> unusedWorkers = new ArrayList<Integer>();
            for (Integer i : newActiveIDs) {
                if (!workersInUse.contains(i)) {
                    // this is an available node
                    unusedWorkers.add(i);
                }
            }

            // distribute task to the available worker(s)
            ArrayList<Task> tmp = new ArrayList<Task>();
            for (Task t : inactive_task_list) {
                if (unusedWorkers.size() > 0) {
                    // set the task
                    int id = unusedWorkers.get(0);
                    t.setNodeID(id);
                    t.setProgress(0);

                    // send the payload
                    String msg = "maple-payload_" + this.sdfsServer.getIP() + "_" + t.getTaskID() + "_" + maple_exe + "_" + sdfs_src_directory + "_";
                    for (String f : t.getInputFiles()) {
                        msg += f + "|";
                    }
                    sendMSG(this.socket, msg, this.sdfsServer.getMembershipListMaintainer().getMembership_list().get(t.getNodeID()).getIp(), "distribute-task");
                    workersInUse.add(id);
                    workerTaskMapping.put(id, t);

                    // remove the used worker
                    unusedWorkers.remove(0);
                    tmp.add(t);
                } else {
                    break;
                }
            }
            active_task_list.addAll(tmp);
            inactive_task_list.removeAll(tmp);

            // print progress info
            System.out.println("Job status update:");
            for (Task t : task_list) {
                t.printBriefInfo();
            }
            System.out.println("");
        }
        System.out.println("Maple job is finished.");
    }

    // shuffle
    private void shuffle() {
        // upload intermediate files to the SDFS
        for (Map.Entry<String, String> entry : sdfsToLocalMapping.entrySet()) {
            this.sdfsServer.put(entry.getValue(), entry.getKey());
        }
    }

    // distribute reduce tasks
    private void reduce(String juice_exe, int num_juices, String sdfs_intermediate_filename_prefix, String sdfs_dest_filename, boolean delete_input) {
        // calculate payload for each reduce task
        ArrayList<String> input_file_names = this.sdfsServer.get_dir_meta(sdfs_intermediate_filename_prefix);
        int num_input_files = input_file_names.size();
        int payload = num_input_files / num_juices;

        // generate tasks
        task_list = new ArrayList<Task>();
        ArrayList<Task> active_task_list = new ArrayList<Task>();
        ArrayList<Task> inactive_task_list = new ArrayList<Task>();
        ArrayList<Integer> workersInUse = new ArrayList<Integer>();
        HashMap<Integer, Task> workerTaskMapping = new HashMap<Integer, Task>();
        ArrayList<Integer> activeMemberIDs = this.sdfsServer.getMembershipListMaintainer().getAllActiveMembers();
        activeMemberIDs.removeAll(this.masterList);
        int fileCounter = 0;
        for (int i = 0; i < num_juices; i++) {
            ArrayList<String> inputFiles = new ArrayList<String>();
            if (i < num_input_files % num_juices) {
                for (int j = 0; j < payload + 1; j++) {
                    inputFiles.add(input_file_names.get(fileCounter + j));
                }
                fileCounter += payload + 1;
            } else {
                for (int j = 0; j < payload; j++) {
                    inputFiles.add(input_file_names.get(fileCounter + j));
                }
                fileCounter += payload;
            }
            if (workersInUse.size() < activeMemberIDs.size()) {
                Task task = new Task("Reduce", i + 1, activeMemberIDs.get(workersInUse.size()), inputFiles, 0);
                task_list.add(task);
                active_task_list.add(task);
                workerTaskMapping.put(activeMemberIDs.get(workersInUse.size()), task);
                workersInUse.add(activeMemberIDs.get(workersInUse.size()));
            } else {
                Task task = new Task("Reduce", i + 1, -1, inputFiles, 0);
                task_list.add(task);
                inactive_task_list.add(task);
            }
        }

        // print task info and distribute active tasks to the workers
        for (Task t : task_list) {
            t.printInfo();
            if (t.getNodeID() == -1) {
                continue;
            }
            String msg = "juice-payload_" + this.sdfsServer.getIP() + "_" + t.getTaskID() + "_" + juice_exe + "_" + sdfs_intermediate_filename_prefix + "_";
            for (String f : t.getInputFiles()) {
                msg += f + "|";
            }
            sendMSG(this.socket, msg, this.sdfsServer.getMembershipListMaintainer().getMembership_list().get(t.getNodeID()).getIp(), "distribute-juice-task");
        }

        // tracking the progress
        numFinishedTasks = 0;
        unprocessedCompletedTasks = new CopyOnWriteArrayList<Integer>();
        HashMap<String, String> keyValuePairs = new HashMap<String, String>();
        while (numFinishedTasks < num_juices) {
            // wait for 1 second
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // check if there's a new finished task -> if yes: gather output from the worker
            ArrayList<Integer> tmp_processed = new ArrayList<Integer>();
            for (Integer i : unprocessedCompletedTasks) {
                // fetch output file from the worker
                Task t = task_list.get(i - 1);
                System.out.println("Processing output file from " + t.getOutputFile() + " Node #" + (t.getNodeID() + 1) + "...");
                String currentDirectory = System.getProperty("user.dir");
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                String fileName = "juice_task_" + timestamp.getTime() + "_" + t.getTaskID() + ".output";
                String localPath = currentDirectory.substring(0, currentDirectory.length() - 7) + "sdfs_dir/" + fileName;
                this.sdfsServer.send_request(this.sdfsServer.getMembershipListMaintainer().getMembership_list().get(t.getNodeID()).getIp(), t.getOutputFile(), localPath);
                workersInUse.remove(Integer.valueOf(t.getNodeID()));

                // gather output together
                Scanner input = new Scanner(System.in);
                File file = new File(localPath);
                try {
                    input = new Scanner(file);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                while (input.hasNextLine()) {
                    String line = input.nextLine();
                    keyValuePairs.put(line.split(",")[0], line.split(",")[1]);
                }
                input.close();

                // delete files and commit
                this.sdfsServer.delete(t.getOutputFile());
                t.setCommitted(true);
                numFinishedTasks += 1;
                tmp_processed.add(i);
                System.out.println("Processing output file " + t.getOutputFile() + " from Node #" + (t.getNodeID() + 1) + " completed.");
            }
            unprocessedCompletedTasks.removeAll(tmp_processed);

            // remove idle workers and find new available worker(s)
            ArrayList<Integer> newActiveIDs = this.sdfsServer.getMembershipListMaintainer().getAllActiveMembers();
            newActiveIDs.removeAll(masterList);
            ArrayList<Integer> tmp_workers = new ArrayList<Integer>();
            for (Integer i : workersInUse) {
                if (!newActiveIDs.contains(i)) {
                    // this is an idle worker
                    tmp_workers.add(i);

                    // put that task to inactive
                    workerTaskMapping.get(i).setNodeID(-1);
                    workerTaskMapping.get(i).setProgress(-1);
                    Task t = workerTaskMapping.get(i);
                    workerTaskMapping.remove(i);
                    active_task_list.remove(t);
                    inactive_task_list.add(t);
                }
            }
            workersInUse.removeAll(tmp_workers);
            ArrayList<Integer> unusedWorkers = new ArrayList<Integer>();
            for (Integer i : newActiveIDs) {
                if (!workersInUse.contains(i)) {
                    // this is an available node
                    unusedWorkers.add(i);
                }
            }

            // distribute task to the available worker(s)
            ArrayList<Task> tmp = new ArrayList<Task>();
            for (Task t : inactive_task_list) {
                if (unusedWorkers.size() > 0) {
                    // set the task
                    int id = unusedWorkers.get(0);
                    t.setNodeID(id);
                    t.setProgress(0);

                    // send the payload
                    String msg = "juice-payload_" + this.sdfsServer.getIP() + "_" + t.getTaskID() + "_" + juice_exe + "_" + sdfs_intermediate_filename_prefix + "_";
                    for (String f : t.getInputFiles()) {
                        msg += f + "|";
                    }
                    sendMSG(this.socket, msg, this.sdfsServer.getMembershipListMaintainer().getMembership_list().get(t.getNodeID()).getIp(), "distribute-task");
                    workersInUse.add(id);
                    workerTaskMapping.put(id, t);

                    // remove the used worker
                    unusedWorkers.remove(0);
                    tmp.add(t);
                } else {
                    break;
                }
            }
            active_task_list.addAll(tmp);
            inactive_task_list.removeAll(tmp);

            // print progress info
            System.out.println("Task status update:");
            for (Task t : task_list) {
                t.printBriefInfo();
            }
            System.out.println("");
        }

        // sort output by key
        TreeMap<String, String> sorted = new TreeMap<>();
        sorted.putAll(keyValuePairs);

        // store in one output file - sdfs_dest_filename
        FileWriter fw = null;
        String currentDirectory = System.getProperty("user.dir");
        String localPath = currentDirectory.substring(0, currentDirectory.length() - 7) + "sdfs_dir/" + sdfs_dest_filename;
        File f = new File(localPath);
        try {
            f.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fw = new FileWriter(f);
        } catch (IOException e) {
            this.sdfsServer.getMembershipListMaintainer().log("[ERROR] - File Writer creation failed!");
            e.printStackTrace();
        }
        for (Map.Entry<String, String> entry : sorted.entrySet()) {
            try {
                assert fw != null;
                fw.write(entry.getKey() + "\t" + entry.getValue() + "\n");
                fw.flush();
            } catch (IOException e) {
                this.sdfsServer.getMembershipListMaintainer().log("[ERROR] - File Writer writing failed!");
                e.printStackTrace();
            }
        }
        try {
            assert fw != null;
            fw.close();
        } catch (IOException e) {
            this.sdfsServer.getMembershipListMaintainer().log("[ERROR] - File Writer closing failed!");
            e.printStackTrace();
        }

        // put it to SDFS
        this.sdfsServer.put(localPath, sdfs_dest_filename);
        System.out.println("The output file is stored at: " + localPath);

        // delete intermediate files if needed
        if (delete_input) {
            for (String s : input_file_names) {
                String fileName = sdfs_intermediate_filename_prefix + "/" + s;
                this.sdfsServer.delete(fileName);
            }
        }
        System.out.println("Juice job is finished.");
    }

    // start the server
    private void start() {
        // start the UDP socket
        try {
            this.socket = new DatagramSocket(port);
        } catch (SocketException e) {
            e.printStackTrace();
        }
        log("MapReduceMaster - UDP socket is created.");

        // start the SDFS server
        SDFSServerThread sdfs = new SDFSServerThread();
        sdfs.start();

        // create threads
        ReceiverThread receiver = new ReceiverThread();
        MonitorThread monitor = new MonitorThread();
        JobExecutorThread executor = new JobExecutorThread();

        // start threads
        receiver.start();
        monitor.start();
        executor.start();

        // wait for termination
        try {
            sdfs.join();
            receiver.join();
            monitor.join();
            executor.join();
        } catch (InterruptedException e) {
            log("[ERROR] - Thread execution interrupted!");
            e.printStackTrace();
        }
    }

    // end the server
    private void end() {
        this.sdfsServer.end();

        // close the socket
        this.socket.close();
    }

    /**
     * Constructor
     */
    public MapReduceMaster(SDFSServer sdfsServer, ArrayList<Integer> masterList) {
        this.sdfsServer = sdfsServer;
        this.masterList = masterList;
        this.jobQueue = new LinkedList<Job>();
    }

    /**
     * SDFSServer thread providing the underlying distributed file system service
     */
    private class SDFSServerThread extends Thread {
        @Override
        public void run() {
            MapReduceMaster.this.sdfsServer.start();
        }
    }

    /**
     * Receiver thread watching on port 6001 for receiving messages from the MapReduce workers
     */
    private class ReceiverThread extends Thread {
        @Override
        public void run() {
            while (!sdfsServer.getShouldExit()) {
                if (sdfsServer.getStatus() != 1) {
                    continue;
                }

                // get UDP message
                byte[] data = new byte[1024];
                DatagramPacket packet = new DatagramPacket(data, data.length);
                try {
                    socket.receive(packet);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String info = new String(data, 0, packet.getLength());
                String[] messages = info.split("_");

                // if the node is idle, skip the receiver
                if (sdfsServer.getStatus() != 1) {
                    continue;
                }

                // parse the message - type_ip_content
                String msg_type = messages[0];
                String src_ip = messages[1];
                log("Received message from " + src_ip + " - " + msg_type);

                switch (msg_type) {
                    case "progress":
                        // progress update message - format: progress_ip_taskID_0.6
                        int taskID = Integer.parseInt(messages[2]);
                        double progress = Double.parseDouble(messages[3]);
                        if (progress > 1) {
                            progress = 1.0;
                        }

                        // update progress in task
                        MapReduceMaster.this.task_list.get(taskID - 1).setProgress(progress);
                        break;
                    case "finished":
                        // task finished message - format: finished_ip_taskID_output
                        taskID = Integer.parseInt(messages[2]);
                        String outputFileName = messages[3];

                        // update progress and output file in task
                        MapReduceMaster.this.task_list.get(taskID - 1).setProgress(1.0);
                        MapReduceMaster.this.task_list.get(taskID - 1).setOutputFile(outputFileName);

                        // add to the unprocessed task list
                        MapReduceMaster.this.unprocessedCompletedTasks.add(taskID);
                        break;
                    case "maple":
                        // map request redirected from the worker
                        String command = messages[2];
                        String maple_exe = command.split("-")[0];
                        int num_maples = Integer.parseInt(command.split("-")[1]);
                        String sdfs_intermediate_filename_prefix = command.split("-")[2];
                        String sdfs_src_directory = command.split("-")[3];
                        System.out.println("Received Maple job from worker - " + src_ip + ": maple " + maple_exe + " " + num_maples + " " + sdfs_intermediate_filename_prefix + " " + sdfs_src_directory + "\n");

                        // add this job to the queue
                        MapReduceMaster.this.jobQueue.add(new Job(1, maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory));
                        System.out.println("Added this Maple job to the queue!");
                        break;
                    case "juice":
                        // reduce request redirected from the worker
                        command = messages[2];
                        String juice_exe = command.split("-")[0];
                        int num_juices = Integer.parseInt(command.split("-")[1]);
                        sdfs_intermediate_filename_prefix = command.split("-")[2];
                        String sdfs_dest_filename = command.split("-")[3];
                        int delete_input = Integer.parseInt(command.split("-")[4]);
                        System.out.println("Received Juice job from worker - " + src_ip + ": juice " + juice_exe + " " + num_juices + " " + sdfs_intermediate_filename_prefix + " " + sdfs_dest_filename + " " + delete_input + "\n");

                        // add this job to the queue
                        MapReduceMaster.this.jobQueue.add(new Job(2, juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input));
                        System.out.println("Added this Juice job to the queue!");
                        break;
                }
            }
        }
    }

    /**
     * Monitor thread watching for user input commands
     * Commands: join, leave, list, info, exit, maple, juice
     */
    private class MonitorThread extends Thread {
        @Override
        public void run() {
            String command;
            Scanner in;
            in = new Scanner(System.in);
            String commandInfo = "\nEnter the command: \n" +
                    " - join: join the group\n" +
                    " - leave: leave the group\n" +
                    " - list: list all the members in the group (active and idle) and its neighbors\n" +
                    " - info: print current server id, neighbors, on-time, etc.\n" +
                    " - exit: exit the command line environment (make sure the the node's status is idle/leave)\n" +
                    " - put [local_file_name] [sdfs_file_name]: insert or update file\n" +
                    " - get [sdfs_file_name] [local_file_name]: fetch file to local\n" +
                    " - get-dir [sdfs_dir] [local_dir]: fetch all files under sdfs_dir to local_dir\n" +
                    " - delete [sdfs_file_name]: delete a file\n" +
                    " - ls [sdfs_file_name]: list all the machines where the file is currently being stored\n" +
                    " - store: list all files which are currently being stored at this machine\n" +
                    " - maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>: map in MapReduce\n" +
                    " - juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}: reduce in MapReduce\n";
            System.out.println(commandInfo);
            command = in.nextLine();
            label:
            while (true) {
                MapReduceMaster.this.log("[INFO] - received command: " + command);
                switch (command.split(" ")[0]) {
                    case "exit":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // status is active, unable to exit
                            System.out.println("Unable to exit because the node is still alive.");
                        } else {
                            // notify other threads to finish and then exit
                            MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().setShouldExit(true);
                            MapReduceMaster.this.log("[INFO] - exited ");
                            System.out.println("Successfully exited.");
                            break label;
                        }
                        break;
                    case "list":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // list all vms
                            MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().listAllMembers();

                            // list all neighbors
                            MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().listAllNeighbors();
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "info":
                        // print info for local server
                        MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().printInfo();
                        break;
                    case "join":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // already in the group
                            System.out.println("Already in the group, no action to take.");
                        } else {
                            // delete all files in the directory before rejoining
                            MapReduceMaster.this.sdfsServer.getLocalSDFSFileList().clear();
                            MapReduceMaster.this.sdfsServer.getSDFSFileList().clear();

                            // join the group via the introducer
                            HashMap<String, SDFSFile> SDFSFileList = null;
                            SDFSFileList = MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().join();
                            if (SDFSFileList != null) {
                                // update SDFSFileList
                                MapReduceMaster.this.sdfsServer.setSDFSFileList(SDFSFileList);
                                System.out.println("Joined successfully!");
                                MapReduceMaster.this.log("[INFO] - Joined successfully");
                            } else {
                                System.out.println("Join-request failed, please try again!");
                                MapReduceMaster.this.log("[INFO] - failed to join");
                            }
                        }
                        break;
                    case "leave":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // leave the group
                            MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().leave();
                            System.out.println("Left successfully!");
                        } else {
                            System.out.println("Already out of the group, no action to take.");
                        }
                        break;
                    case "put":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // insert or update a file
                            if (command.split(" ").length != 3) {
                                System.out.println("Wrong number of arguments (put [local_file_name] [sdfs_file_name])!");
                            } else {
                                String local_filename = command.split(" ")[1];
                                String sdfs_filename = command.split(" ")[2];
                                File f = new File(local_filename);
                                if (!f.exists()) {
                                    System.out.println("File does not exist!");
                                } else {
                                    sdfsServer.put(local_filename, sdfs_filename);
                                    System.out.println("Put-request is successfully executed on the SDFS - " + sdfs_filename);
                                }
                            }
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "get":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // retrieve a file
                            if (command.split(" ").length != 3) {
                                System.out.println("Wrong number of arguments (get [sdfs_file_name] [local_file_name])!");
                            } else {
                                String local_filename = command.split(" ")[2];
                                String sdfs_filename = command.split(" ")[1];
                                sdfsServer.get(sdfs_filename, local_filename);
                                System.out.println("Get-request is successfully executed on the SDFS - " + sdfs_filename);
                                System.out.println("File requested is located at " + local_filename);
                            }
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "delete":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // delete a file
                            if (command.split(" ").length != 2) {
                                System.out.println("Wrong number of arguments (delete [sdfs_file_name])!");
                            } else {
                                String sdfs_filename = command.split(" ")[1];
                                sdfsServer.delete(sdfs_filename);
                                System.out.println("Delete-request is successfully executed on the SDFS - " + sdfs_filename);
                            }
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "ls":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // list all machines where a file is stored on the SDFS
                            if (command.split(" ").length != 2) {
                                System.out.println("Wrong number of arguments (ls [sdfs_file_name])!");
                            } else {
                                String sdfs_filename = command.split(" ")[1];
                                sdfsServer.listFile(sdfs_filename);
                            }
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "store":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // list all files which are currently being stored at this machine
                            sdfsServer.listStore();
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "maple":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // maple task (map)
                            if (command.split(" ").length != 5) {
                                System.out.println("Wrong number of arguments (maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>)!");
                            } else {
                                String maple_exe = command.split(" ")[1];
                                int num_maples = Integer.parseInt(command.split(" ")[2]);
                                String sdfs_intermediate_filename_prefix = command.split(" ")[3];
                                String sdfs_src_directory = command.split(" ")[4];

                                // add the job to the queue
                                MapReduceMaster.this.jobQueue.add(new Job(1, maple_exe, num_maples, sdfs_intermediate_filename_prefix, sdfs_src_directory));
                                System.out.println("Added this Maple job to the queue!");
                            }
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "juice":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // juice task (reduce)
                            if (command.split(" ").length != 6) {
                                System.out.println("Wrong number of arguments (juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1})!");
                            } else {
                                String juice_exe = command.split(" ")[1];
                                int num_juices = Integer.parseInt(command.split(" ")[2]);
                                String sdfs_intermediate_filename_prefix = command.split(" ")[3];
                                String sdfs_dest_filename = command.split(" ")[4];
                                int delete_input = Integer.parseInt(command.split(" ")[5]);

                                // add the job to the queue
                                MapReduceMaster.this.jobQueue.add(new Job(2, juice_exe, num_juices, sdfs_intermediate_filename_prefix, sdfs_dest_filename, delete_input));
                                System.out.println("Added this Juice job to the queue!");
                            }
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "get-dir":
                        if (MapReduceMaster.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // fetch a directory
                            if (command.split(" ").length != 3) {
                                System.out.println("Wrong number of arguments (get-dir [sdfs_dir] [local_dir])!");
                            } else {
                                String local_dir = command.split(" ")[2];
                                String sdfs_dir = command.split(" ")[1];
                                sdfsServer.get_dir(sdfs_dir, local_dir);
                            }
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    // for demo
                    case "demo-wc":
                        System.out.println("Uploading input data and maple.exe...\n");
                        sdfsServer.put("/home/haoranq4/mini-projs/MP4/applications/wc-maple.py", "maple.exe");
                        for (int i = 1; i <= 9; i++) {
                            sdfsServer.put("/home/haoranq4/mini-projs/MP4/test-dir/wc-input/wc" + i + ".txt", "mjwc/input" + i);
                            System.out.println("...");
                        }
                        MapReduceMaster.this.jobQueue.add(new Job(1, "maple.exe", 9, "prefix", "mjwc"));
                        System.out.println("Uploading juice.exe");
                        sdfsServer.put("/home/haoranq4/mini-projs/MP4/applications/wc-juice.py", "juice.exe");
                        MapReduceMaster.this.jobQueue.add(new Job(2, "juice.exe", 9, "prefix", "output.txt", 0));
                        break;
                    case "upload-data":
                        System.out.println("Uploading maple.exe...");
                        sdfsServer.put("/home/haoranq4/mini-projs/MP4/applications/wc-maple.py", "maple.exe");
                        System.out.println("Uploading input files...");
                        for (int i = 1; i <= 9; i++) {
                            sdfsServer.put("/home/haoranq4/mini-projs/MP4/test-dir/wc-input/wc" + i + ".txt", "mjwc/input" + i);
                            System.out.println("...");
                        }
                        System.out.println("Input files are uploaded to mjwc directory in the SDFS.");
                        System.out.println("Uploading juice.exe...");
                        sdfsServer.put("/home/haoranq4/mini-projs/MP4/applications/wc-juice.py", "juice.exe");
                        break;
                    default:
                        System.out.println("Wrong command!");
                        break;
                }

                // enter the next query command
                System.out.println(commandInfo);
                command = in.nextLine();
            }
        }
    }

    /**
     * JobExecutor thread executing each job in the queue in sequence
     */
    private class JobExecutorThread extends Thread {
        @Override
        public void run() {
            while (!sdfsServer.getShouldExit() && sdfsServer.getStatus() == 1) {
                if (jobQueue.size() == 0) {
                    continue;
                }
                // dequeue a job
                Job currentJob = jobQueue.remove();
                if (currentJob.getJobType() == 1) {
                    // map job
                    System.out.println("Executing Map Job - maple " + currentJob.getJobExe() + " " + currentJob.getNumTasks() + " " + currentJob.getSdfs_intermediate_filename_prefix() + " " + currentJob.getSdfs_src_directory() + "\n");
                    // long start_time = System.currentTimeMillis();
                    map(currentJob.getJobExe(), currentJob.getNumTasks(), currentJob.getSdfs_intermediate_filename_prefix(), currentJob.getSdfs_src_directory());
                    // long end_time = System.currentTimeMillis();
                    // System.out.println("Time consumed: " + (end_time - start_time) / 1000F + " seconds");
                } else if (currentJob.getJobType() == 2) {
                    // reduce job
                    System.out.println("Shuffling...");
                    // long start_time = System.currentTimeMillis();
                    shuffle();
                    System.out.println("Shuffling completed.");
                    // long end_time = System.currentTimeMillis();
                    // System.out.println("Time consumed: " + (end_time - start_time) / 1000F + " seconds");
                    System.out.println("Executing Juice Job - juice " + currentJob.getJobExe() + " " + currentJob.getNumTasks() + " " + currentJob.getSdfs_intermediate_filename_prefix() + " " + currentJob.getSdfs_dest_directory() + " " + currentJob.getDeleteInput() + "\n");
                    // start_time = System.currentTimeMillis();
                    reduce(currentJob.getJobExe(), currentJob.getNumTasks(), currentJob.getSdfs_intermediate_filename_prefix(), currentJob.getSdfs_dest_directory(), currentJob.getDeleteInput() != 0);
                    // end_time = System.currentTimeMillis();
                    // System.out.println("Time consumed: " + (end_time - start_time) / 1000F + " seconds");
                }
            }
        }
    }

    /**
     * Main function
     */
    public static void main(String[] args) {
        // server id
        int index = Integer.parseInt(args[0]);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String id = String.format("%02d", index) + "-" + Long.toString(timestamp.getTime());

        // vm information
        ArrayList<String> vm_list = new ArrayList<String>();
        for (int i = 1; i <= 10; i++) {
            if (i != 10) {
                vm_list.add("fa19-cs425-g46-0" + Integer.toString(i) + ".cs.illinois.edu");
            } else {
                vm_list.add("fa19-cs425-g46-" + Integer.toString(i) + ".cs.illinois.edu");
            }
        }

        // create the server service
        MembershipListMaintainer s = null;
        HashMap<String, SDFSFile> SDFSFileList = null;

        // mode for starting a server - 1 for bootstrap mode & 2 for normal node
        int mode = Integer.parseInt(args[1]);

        if (mode == 1) {
            // bootstrap mode
            s = new MembershipListMaintainer(String.format("%02d", index) + "-0", vm_list.get(index - 1), vm_list);
            s.bootstrap(index);
        } else if (mode == 2) {
            // normal mode - contact the introducer
            s = new MembershipListMaintainer(id, vm_list.get(index - 1), vm_list);

            SDFSFileList = s.join();
            if (SDFSFileList != null) {
                System.out.println("Joined successfully!");
                s.log("[INFO] " + s.server_id_in_log() + "Joined successfully");
            } else {
                System.out.println("Join-request failed, please try again!");
                s.log("[INFO] " + s.server_id_in_log() + "fail to join");
            }
        }

        // set the joining timestamp
        assert s != null;
        s.setJoin_timestamp(timestamp.getTime());

        System.out.println("\nServer ID: " + index + " (Master Node)");
        System.out.println("Mode: " + (mode == 1 ? "Bootstrap Mode" : "Normal Mode"));

        // create the SDFS server
        SDFSServer sdfsServer = null;
        sdfsServer = new SDFSServer(s, index, SDFSFileList);

        // create the master server
        ArrayList<Integer> masterList = new ArrayList<Integer>();
        masterList.add(index - 1);
        MapReduceMaster master = new MapReduceMaster(sdfsServer, masterList);

        // start the service
        System.out.println("\nStarting the MapReduce Master server ...");
        master.start();

        // end the service
        master.end();
    }
}
