import java.io.*;
import java.net.*;
import java.sql.Timestamp;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.text.DecimalFormat;

public class MapReduceWorker {
    // SDFS file server
    private SDFSServer sdfsServer;

    // UDP port for sending messages to the MapReduce master
    private static final int port = 6001;

    // UDP socket programming
    private DatagramSocket socket;

    // IP of the Master node
    private String masterIP = "fa19-cs425-g46-01.cs.illinois.edu";

    // progress of the task
    private double progress;
    private DecimalFormat df = new DecimalFormat("#.####");

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

    // on receiving a map task
    private void map(String taskID, String IP, String sdfs_dir, HashSet<String> inputFiles, String maple_exe) {
        System.out.println("Start to work on Task #" + taskID + "...");
        // retrieve all input files from SDFS
        String currentDirectory = System.getProperty("user.dir");
        String local_dir = currentDirectory.substring(0, currentDirectory.length() - 7) + "sdfs_dir";
        System.out.println("Dowloading input files into " + local_dir + "...");
        System.out.print("[ ");
        for (String s : inputFiles) {
            System.out.print(s + " ");
        }
        this.sdfsServer.get_part_dir(sdfs_dir, local_dir, inputFiles);
        System.out.println("]\nDowloading input files into " + local_dir + " - Done");

        // fetch maple_exe to local
        this.sdfsServer.get(maple_exe, local_dir + "/" + maple_exe);

        // count the total number of input lines
        long total_num_lines = 1;
        try {
            for (String s : inputFiles) {
                Path path = Paths.get(local_dir + "/" + s);
                total_num_lines += Files.lines(path).count();
            }
        } catch (IOException e) {
            e.printStackTrace();
            log("[ERROR] - Maple Get file size error");
        }

        // create output file
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String filename = local_dir + "/worker_task" + taskID + "_" + timestamp.getTime() + "_maple_output";
        File output = new File(filename);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(output));
        } catch (IOException e) {
            e.printStackTrace();
            log("[ERROR] - Maple File Writer creation failed!");
        }

        // execute maple_exe on each input file
        progress = 0;
        int count = 0;
        long executed_num_lines = 0;
        long prev_executed_num_lines = 0;
        for (String f : inputFiles) {
            String localPath = local_dir + "/" + f;
            // call maple_exe for every 10 lines and get results
            try {
                File file = new File(localPath);
                BufferedReader in = new BufferedReader(new FileReader(file));
                String str;
                int times = (int) (Files.lines(Paths.get(localPath)).count() / 10) + 1;
                count = 0;
                String exe_string = "";
                while ((str = in.readLine()) != null) {
                    exe_string += str + "\n";
                    count += 1;
                    if (count == 10) {
                        count = 0;
                        String[] cmd = {"python", local_dir + "/" + maple_exe, exe_string};
                        Process process = Runtime.getRuntime().exec(cmd);

                        // read the result
                        BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
                        String l = null;
                        while ((l = stdout.readLine()) != null) {
                            // write to the file
                            out.write(l + '\n');
                        }
                        exe_string = "";

                        // update progress
                        executed_num_lines += 10;
                        progress = (double) executed_num_lines / total_num_lines;
                        if (executed_num_lines - prev_executed_num_lines > 50) {
                            updateProgress(IP, taskID);
                            log("[INFO] - task" + taskID + " sending maple progress message to the master");
                            prev_executed_num_lines = executed_num_lines;
                        }
                    }
                }
                if (exe_string.length() > 0) {
                    // process the remaining lines
                    String[] cmd = {"python", local_dir + "/" + maple_exe, exe_string};
                    Process process = Runtime.getRuntime().exec(cmd);

                    // read the result
                    BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String l = null;
                    while ((l = stdout.readLine()) != null) {
                        // write to the file
                        out.write(l + '\n');
                    }

                    // update progress
                    executed_num_lines += 10;
                    progress = (double) executed_num_lines / total_num_lines;
                    if (executed_num_lines - prev_executed_num_lines > 50) {
                        updateProgress(IP, taskID);
                        log("[INFO] - task" + taskID + " sending maple progress message to the master");
                        prev_executed_num_lines = executed_num_lines;
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
                log("[ERROR] - Maple file write error");
            }
        }

        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            log("[ERROR] - Maple file writer close failed!");
        }

        // upload the file to sdfs
        String outputFileName = "task" + taskID + "~maple~" + timestamp.getTime();
        this.sdfsServer.put(filename, outputFileName);
        System.out.println("Task # " + taskID + " Finished - Output File Uploaded - " + outputFileName);

        // after completion - send message to the master (finished_ip_taskID_outputFileName)
        String msg = "finished_" + this.sdfsServer.getIP() + "_" + taskID + "_" + outputFileName;
        this.sendMSG(this.socket, msg, IP, "task-finished");
        log("[INFO] - task" + taskID + " completed - successfully uploaded output file to SDFS");
    }

    // on receiving a reduce task
    private void reduce(String taskID, String IP, String prefix, HashSet<String> inputFiles, String juice_exe) {
        System.out.println("Start to work on Task #" + taskID + "...");
        // retrieve all input files from SDFS
        String currentDirectory = System.getProperty("user.dir");
        String local_dir = currentDirectory.substring(0, currentDirectory.length() - 7) + "sdfs_dir";
        System.out.println("Dowloading input files...");
        System.out.print("[ ");
        for (String s : inputFiles) {
            System.out.print(s + " ");
        }
        this.sdfsServer.get_part_dir(prefix, local_dir, inputFiles);
        System.out.println("]\nInput file downloaded to " + local_dir);

        // fetch juice_exe to local
        this.sdfsServer.get(juice_exe, local_dir + "/juice.exe");

        // count the total number of input lines
        long total_num_lines = 1;
        try {
            for (String s : inputFiles) {
                Path path = Paths.get(local_dir + "/" + s);
                total_num_lines += Files.lines(path).count();
            }
        } catch (IOException e) {
            e.printStackTrace();
            log("[ERROR] - Juice get file size error");
        }

        // create output file
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String filename = local_dir + "/worker_task" + taskID + "_" + timestamp.getTime() + "_juice_output";
        File output = new File(filename);
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new FileWriter(output));
        } catch (IOException e) {
            e.printStackTrace();
            log("[ERROR] - Juice file writer creation failed!");
        }

        // execute juice_exe on each input file
        progress = 0;
        long executed_num_lines = 0;
        long prev_executed_num_lines = 0;
        for (String f : inputFiles) {
            String key = null;
            String localPath = local_dir + "/" + f;
            try {
                File file = new File(localPath);
                BufferedReader in = new BufferedReader(new FileReader(file));
                int file_size = (int) (Files.lines(Paths.get(localPath)).count());
                String[] cmd = new String[4];
                cmd[0] = "python";
                cmd[1] = local_dir + "/juice.exe";
                cmd[2] = f;
                cmd[3] = localPath;
                Process process = Runtime.getRuntime().exec(cmd);

                // read the result
                BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String l;
                while ((l = stdout.readLine()) != null) {
                    // write to the file
                    out.write(l + '\n');
                }

                // update progress
                executed_num_lines += file_size;
                progress = (double) executed_num_lines / total_num_lines;
                if (executed_num_lines - prev_executed_num_lines > 50) {
                    updateProgress(IP, taskID);
                    log("[INFO] - task" + taskID + " sending juice progress message to the master");
                    prev_executed_num_lines = executed_num_lines;
                }
            } catch (IOException e) {
                e.printStackTrace();
                log("[ERROR] - Juice file write error");
            }
        }

        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            log("[ERROR] - Juice file writer close failed!");
        }

        // upload the file to sdfs
        String outputFileName = "task" + taskID + "~juice~" + timestamp.getTime();
        this.sdfsServer.put(filename, outputFileName);
        System.out.println("Task # " + taskID + " Finished - Output File Uploaded - " + outputFileName);

        // after completion - send message to the master (finished_ip_taskID_outputFileName)
        String msg = "finished_" + this.sdfsServer.getIP() + "_" + taskID + "_" + outputFileName;
        this.sendMSG(this.socket, msg, IP, "task-finished");
        log("[INFO] - task" + taskID + " completed, successfully uploaded output of juice");
    }

    // update task progress to the Master
    private void updateProgress(String src_ip, String taskID) {
        // message format: progress_ip_taskID_taskProgress
        String msg = "progress_" + this.sdfsServer.getIP() + "_" + taskID + "_" + df.format(progress);
        this.sendMSG(this.socket, msg, src_ip, "task-progress-report");
    }

    // start the server
    private void start() {
        // start the UDP socket
        try {
            this.socket = new DatagramSocket(port);
        } catch (SocketException e) {
            log("[ERROR] - Socket creation failed!");
            e.printStackTrace();
        }
        log("MapReduceWorker - UDP socket is created.");

        // start the SDFS server
        SDFSServerThread sdfs = new SDFSServerThread();
        sdfs.start();

        // create threads
        ReceiverThread receiver = new ReceiverThread();
        MonitorThread monitor = new MonitorThread();

        // start threads
        receiver.start();
        monitor.start();

        // wait for termination
        try {
            sdfs.join();
            receiver.join();
            monitor.join();
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
    public MapReduceWorker(SDFSServer sdfsServer) {
        this.sdfsServer = sdfsServer;
    }

    /**
     * SDFSServer thread providing the underlying distributed file system service
     */
    private class SDFSServerThread extends Thread {
        @Override
        public void run() {
            MapReduceWorker.this.sdfsServer.start();
        }
    }

    /**
     * Receiver thread watching on port 6001 for receiving messages from the MapReduce master
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
                String taskID = messages[2];
                log("Received message from " + src_ip + " - " + msg_type + " - Task ID = " + taskID);

                switch (msg_type) {
                    case "maple-payload":
                        // Maple task assignment message - format: maple-payload_ip_exe_file1|file2|file3|
                        String maple_exe = messages[3];
                        String sdfs_dir = messages[4];
                        HashSet<String> inputMapleFiles = new HashSet<String>();
                        Collections.addAll(inputMapleFiles, messages[5].split("\\|"));
                        // start doing the Maple task
                        MapReduceWorker.this.map(taskID, src_ip, sdfs_dir, inputMapleFiles, maple_exe);
                        break;
                    case "juice-payload":
                        // Juice task assignment message - format: maple-payload_ip_exe_file1|file2|file3|
                        String juice_exe = messages[3];
                        String prefix = messages[4];
                        HashSet<String> inputJuiceFiles = new HashSet<String>();
                        Collections.addAll(inputJuiceFiles, messages[5].split("\\|"));

                        // start doing the Juice task
                        MapReduceWorker.this.reduce(taskID, src_ip, prefix, inputJuiceFiles, juice_exe);
                        break;
                    case "update-request":
                        // task progress request message
                        // report the progress of the current task
                        MapReduceWorker.this.updateProgress(src_ip, taskID);
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
                    " - maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>: redirect to the Master\n" +
                    " - juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}: redirect to the Master\n";
            System.out.println(commandInfo);
            command = in.nextLine();
            label:
            while (true) {
                MapReduceWorker.this.log("[INFO] - received command: " + command);
                switch (command.split(" ")[0]) {
                    case "exit":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // status is active, unable to exit
                            System.out.println("Unable to exit because the node is still alive.");
                        } else {
                            // notify other threads to finish and then exit
                            MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().setShouldExit(true);
                            MapReduceWorker.this.log("[INFO] - exited ");
                            System.out.println("Successfully exited.");
                            break label;
                        }
                        break;
                    case "list":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // list all vms
                            MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().listAllMembers();

                            // list all neighbors
                            MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().listAllNeighbors();
                        } else {
                            System.out.println("Unable to execute this command because the node is not in the group!");
                        }
                        break;
                    case "info":
                        // print info for local server
                        MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().printInfo();
                        break;
                    case "join":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // already in the group
                            System.out.println("Already in the group, no action to take.");
                        } else {
                            // delete all files in the directory before rejoining
                            MapReduceWorker.this.sdfsServer.getLocalSDFSFileList().clear();
                            MapReduceWorker.this.sdfsServer.getSDFSFileList().clear();

                            // join the group via the introducer
                            HashMap<String, SDFSFile> SDFSFileList = null;
                            SDFSFileList = MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().join();
                            if (SDFSFileList != null) {
                                // update SDFSFileList
                                MapReduceWorker.this.sdfsServer.setSDFSFileList(SDFSFileList);
                                System.out.println("Joined successfully!");
                                MapReduceWorker.this.log("[INFO] - Joined successfully");
                            } else {
                                System.out.println("Join-request failed, please try again!");
                                MapReduceWorker.this.log("[INFO] - failed to join");
                            }
                        }
                        break;
                    case "leave":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // leave the group
                            MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().leave();
                            System.out.println("Left successfully!");
                        } else {
                            System.out.println("Already out of the group, no action to take.");
                        }
                        break;
                    case "put":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
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
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "get":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
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
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "delete":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // delete a file
                            if (command.split(" ").length != 2) {
                                System.out.println("Wrong number of arguments (delete [sdfs_file_name])!");
                            } else {
                                String sdfs_filename = command.split(" ")[1];
                                sdfsServer.delete(sdfs_filename);
                                System.out.println("Delete-request is successfully executed on the SDFS - " + sdfs_filename);
                            }
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "ls":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
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
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // list all files which are currently being stored at this machine
                            sdfsServer.listStore();
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "maple":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // maple task (map)
                            if (command.split(" ").length != 5) {
                                System.out.println("Wrong number of arguments (maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>)!");
                            } else {
                                String maple_exe = command.split(" ")[1];
                                String num_maples = command.split(" ")[2];
                                String sdfs_intermediate_filename_prefix = command.split(" ")[3];
                                String sdfs_src_directory = command.split(" ")[4];
                                //upload the maple_exe to the sdfs
                                sdfsServer.put(maple_exe, maple_exe);
                                String msg = "maple_" + sdfsServer.getIP() + "_" + maple_exe + "-" + num_maples + "-" + sdfs_intermediate_filename_prefix + "-" + sdfs_src_directory;
                                // redirect the request to the Master
                                int ret = -1;
                                while (ret == -1) {
                                    ret = sendMSG(socket, msg, masterIP, "redirect-maple");
                                }
                                System.out.println("Maple request successfully redirected to the Master node!");
                            }
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "juice":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
                            // juice task (reduce)
                            if (command.split(" ").length != 6) {
                                System.out.println("Wrong number of arguments (juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1})!");
                            } else {
                                String juice_exe = command.split(" ")[1];
                                String num_juices = command.split(" ")[2];
                                String sdfs_intermediate_filename_prefix = command.split(" ")[3];
                                String sdfs_dest_filename = command.split(" ")[4];
                                String delete_input = command.split(" ")[5];
                                //upload the juice_exe to the sdfs
                                sdfsServer.put(juice_exe, juice_exe);

                                String msg = "juice_" + sdfsServer.getIP() + "_" + juice_exe + "-" + num_juices + "-" + sdfs_intermediate_filename_prefix + "-" + sdfs_dest_filename + "-" + delete_input;

                                // redirect the request to the Master
                                int ret = -1;
                                while (ret == -1) {
                                    ret = sendMSG(socket, msg, masterIP, "redirect-juice");
                                }
                                System.out.println("Juice request successfully redirected to the Master node!");
                            }
                        } else {
                            System.out.println("Unable to execute the command because the node is not in the group!");
                        }
                        break;
                    case "get-dir":
                        if (MapReduceWorker.this.sdfsServer.getMembershipListMaintainer().getStatus() == 1) {
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
                    case "word-count-1m":
                        // for demo - not for production
                        System.out.println("Experiment - Word Count with 26 1M input data files:\n");
                        for (int i = 0; i < 26; i++) {
                            sdfsServer.put("/home/haoranq4/mini-projs/MP4/test-files/word-count-1m/wordcount1m." + String.format("%02d", i), "mj~wc~1m/input" + i);
                        }
                        break;
                    case "word-count-2m":
                        // for demo - not for production
                        System.out.println("Experiment - Word Count with 13 2M input data files:\n");
                        for (int i = 0; i < 13; i++) {
                            sdfsServer.put("/home/haoranq4/mini-projs/MP4/test-files/word-count-2m/wordcount2m." + String.format("%02d", i), "mj~wc~2m/input" + i);
                        }
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

        System.out.println("\nServer ID: " + index + " (Worker Node)");
        System.out.println("Mode: " + (mode == 1 ? "Bootstrap Mode" : "Normal Mode"));

        // create the SDFS server
        SDFSServer sdfsServer = null;
        sdfsServer = new SDFSServer(s, index, SDFSFileList);

        // create the master server
        MapReduceWorker worker = new MapReduceWorker(sdfsServer);

        // start the service
        System.out.println("\nStarting the MapReduce Worker server ...");
        worker.start();

        // end the service
        worker.end();
    }
}
