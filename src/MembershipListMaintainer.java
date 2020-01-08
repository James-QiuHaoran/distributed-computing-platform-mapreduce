import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.*;
import java.sql.Array;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * MembershipListMaintainer class for distributed membership protocol.
 */
public class MembershipListMaintainer {
    private static final int port = 3001;            // port number for receiving msgs
    private static final int port_introducer = 3002; // port number of introducer
    private static final int port_SDFSserver = 5001; // port number of SDFS server
    private static final long timeout_fail = 2000;   // timeout for failure
    private static final long timeout_clean = 2000;  // timeout for clean up

    // for logging
    private File f = null;
    private FileWriter fw = null;

    // for exist
    private volatile boolean shouldExit = false;

    // set shouldExit
    public void setShouldExit(boolean shouldExit) {
        this.shouldExit = shouldExit;
    }

    // get shouldExit
    public boolean getShouldExit() {
        return shouldExit;
    }

    // UDP socket programming
    private DatagramSocket socket;
    private byte[] buf;

    // server id - vm_id + timestamp_joining
    private String id;

    // local ip address
    private String ip;

    // server status
    private int status;

    // joining timestamp
    private long join_timestamp;

    // full membership list
    private ArrayList<Node> membership_list = null;

    // direct neighbors (2 successors and 2 predecessors)
    private ArrayList<Integer> neighbor_list = null;

    // vm info
    private ArrayList<String> vm_list = null;

    // used for report experiment
    private boolean isExperiment = false;
    private int threshold = 0; // 3, 10, 30

    // get the server id
    public String getId() {
        return id;
    }

    // set the joining timestamp
    public void setJoin_timestamp(long timestamp) {
        this.join_timestamp = timestamp;
    }

    // set status
    public void setStatus(int status) {
        this.status = status;
    }

    //get status
    public int getStatus() {
        return status;
    }

    // log identifier
    public String server_id_in_log() {
        return "[" + id + ":" + ip + "]";
    }

    // retrieve all members in the membership list
    public ArrayList<Node> getMembership_list() {
        return membership_list;
    }

    // set membership list
    public void setMembership_list(ArrayList<Node> membership_list) {
        this.membership_list = membership_list;
    }

    // get a particular member
    public Node getMember(int i) {
        return membership_list.get(i);
    }

    public int getNumberOfActiveMembers() {
        int count = 0;
        for (Node n : membership_list) {
            if (n.getStatus() == 1) {
                count++;
            }
        }
        return count;
    }

    // get all direct neighbors
    public ArrayList<Integer> getNeighbor_list() {
        return neighbor_list;
    }

    // set all direct neighbors
    public void setNeighbor_list(ArrayList<Integer> neighbor_list) {
        this.neighbor_list = neighbor_list;
    }

    // update the neighbor list
    public void updateNeighbor_list() {
        if (getNumberOfActiveMembers() < 5) {
            return;
        }
        int machine_id = Integer.parseInt(ip.substring(15, 17));
        ArrayList<Integer> neighbors = new ArrayList<Integer>();
        int i = 0;
        int j = 0;
        // get 2 successors
        i = 0;
        j = 0;
        while (i < 2) {
            int id_succ = machine_id + j >= membership_list.size() ? machine_id + j - membership_list.size() : machine_id + j;
            Node successor = membership_list.get(id_succ);
            if (successor.getStatus() == 0) {
                j++;
            } else {
                neighbors.add(id_succ);
                i++;
                j++;
            }
        }

        // get 2 predecessors
        i = 0;
        j = 2;
        while (i < 2) {
            int id_pred = machine_id - j < 0 ? membership_list.size() + machine_id - j : machine_id - j;
            Node predecessor = membership_list.get(id_pred);
            if (predecessor.getStatus() == 0) {
                j++;
            } else {
                neighbors.add(id_pred);
                i++;
                j++;
            }
        }
        setNeighbor_list(neighbors);
    }

    // send via UDP - return 0 if no error when sending, -1 otherwise
    public int send_msg(DatagramSocket socket, String msg, String dest_ip, int dest_port, String message_type) {
        if (isExperiment) {
            // set random number
            threshold = 3; // 3% loss, 10% loss, 30% loss
            Double random = Math.random() * 100;
            if (random <= threshold) {
                return 0; // message dropped
            }
        }
        buf = msg.getBytes();
        DatagramPacket packet;
        try {
            packet = new DatagramPacket(buf, buf.length, InetAddress.getByName(dest_ip), dest_port);
        } catch (UnknownHostException e) {
            log("[ERROR] " + server_id_in_log() + " - error creating UDP packet - unknown host [" + dest_ip + "]");
            e.printStackTrace();
            return -1;
        }
        try {
            socket.send(packet);
        } catch (IOException e) {
            log("[ERROR] " + server_id_in_log() + " - error sending UDP packet [" + dest_ip + "]");
            e.printStackTrace();
        }
        log("[MESSAGE] " + server_id_in_log() + " - message sent to " + dest_ip + ":" + dest_port + " - " + message_type);
        return 0;
    }

    // logging
    public void log(String content) {
        try {
            // timestamp
            String pattern = "MM/dd/yyyy HH:mm:ss";
            DateFormat df = new SimpleDateFormat(pattern);
            Date today = Calendar.getInstance().getTime();
            String now = df.format(today);
            fw.write("[" + now + "] " + content + "\n");
            fw.flush();
        } catch (IOException e) {
            System.err.println("[ERROR] Write to log file failed!");
            e.printStackTrace();
        }
    }

    // bootstrap - six machines
    public void bootstrap(int index) {
        switch (index) {
            case 1:
                // neighbors: 2, 3, 5, 6
                neighbor_list = new ArrayList<Integer>();
                for (int i : new int[]{2, 3, 5, 6}) {
                    neighbor_list.add(i - 1);
                }
                break;
            case 2:
                // neighbors: 3, 4, 6, 1
                for (int i : new int[]{3, 4, 6, 1}) {
                    neighbor_list.add(i - 1);
                }
                break;
            case 3:
                // neighbors: 4, 5, 1, 2
                for (int i : new int[]{4, 5, 1, 2}) {
                    neighbor_list.add(i - 1);
                }
                break;
            case 4:
                // neighbors: 5, 6, 2, 3
                for (int i : new int[]{5, 6, 2, 3}) {
                    neighbor_list.add(i - 1);
                }
                break;
            case 5:
                // neighbors: 6, 1, 3, 4
                for (int i : new int[]{6, 1, 3, 4}) {
                    neighbor_list.add(i - 1);
                }
                break;
            case 6:
                // neighbors: 1, 2, 4, 5
                for (int i : new int[]{1, 2, 4, 5}) {
                    neighbor_list.add(i - 1);
                }
                break;
            default:
                // error
                log("[ERROR] " + server_id_in_log() + " - error in bootstrapping due to wrong index");
        }

        // initial membership list is the vm list
        for (int i = 0; i < vm_list.size(); i++) {
            if (i == index - 1) {
                Node node = new Node(vm_list.get(i), String.format("%02d", i + 1) + "-0", 0, 1);
                membership_list.add(node);
            } else {
                Node node = new Node(vm_list.get(i), String.format("%02d", i + 1) + "-0", 0, 0);
                membership_list.add(node);
            }
        }
        log("[INFO] " + server_id_in_log() + " - bootstrap done");

        // set status to be 1
        this.setStatus(1);
    }

    // join the network via the introducer - return 1 for success and 0 for failure
    public HashMap<String, SDFSFile> join() {
        HashMap<String, SDFSFile> SDFSFileList = null;
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        id = ip.substring(15, 17) + "-" + Long.toString(timestamp.getTime());
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket(port_introducer);
        } catch (SocketException e) {
            e.printStackTrace();
            log("[ERROR] " + server_id_in_log() + " - Introducer Socket creation failed!");
            return SDFSFileList;
        }
        String message = "join-request_" + ip + "_XX_" + id;
        // send message to the introducer
        int ret = send_msg(socket, message, "fa19-cs425-g46-01.cs.illinois.edu", port_SDFSserver, "join");
        if (ret == -1) {
            // error sending the message
            socket.close();
            return SDFSFileList;
        }
        // set the timeout value to be 5 seconds
        try {
            socket.setSoTimeout(5000);
        } catch (SocketException e) {
            e.printStackTrace();
            socket.close();
            return SDFSFileList;
        }
        // receive response from the introducer
        byte[] data = new byte[2048];
        DatagramPacket packet = new DatagramPacket(data, data.length);
        try {
            socket.receive(packet);
        } catch (SocketTimeoutException e) {
            // timeout
            System.out.println("No response from the introducer, please try again later!");
            log("[ERROR] " + server_id_in_log() + " - No response from the introducer");
            socket.close();
            return SDFSFileList;
        } catch (IOException e) {
            e.printStackTrace();
            socket.close();
            return SDFSFileList;
        }

        String info = new String(data, 0, packet.getLength()); // memberList;fileList
        String[] parts = info.split(";");

        // current membership list
        String[] memberList = parts[0].split("_");
        ArrayList<Node> update_list = new ArrayList<Node>();
        // convert the message to membership list
        for (int i = 0; i < memberList.length; i++) {
            String[] node = memberList[i].split("\\|");
            Node nodeinfo = new Node();
            nodeinfo.setIp(node[0]);
            nodeinfo.setId(node[1]);
            nodeinfo.setStatus(Integer.parseInt(node[3]));
            nodeinfo.setTimestamp(Long.parseLong(node[2]));
            update_list.add(nodeinfo);
        }
        for (int j = 0; j < update_list.size(); j++) {
            if (update_list.get(j).getIp() == ip) {
                update_list.get(j).setStatus(status);
                update_list.get(j).setTimestamp(join_timestamp);
                break;
            }
        }
        setMembership_list(update_list);
        updateNeighbor_list();

        // current SDFS file list
        SDFSFileList = new HashMap<String, SDFSFile>();
        if (!parts[1].equals("NONE")) {
            String[] fileList = parts[1].split("_");
            // convert the message to SDFS file list
            for (int i = 0; i < fileList.length; i++) {
                String[] contents = fileList[i].split("\\|");
                SDFSFile file = new SDFSFile(contents[0], null, SDFSFile.stringToReplicas(contents[1]), Long.parseLong(contents[2]));
                SDFSFileList.put(contents[0], file);
            }
        }

        socket.close();

        // set status to active
        this.setStatus(1);

        // update joining_timestamp
        this.setJoin_timestamp(timestamp.getTime());

        return SDFSFileList;
    }

    // leave the network
    public void leave() {
        // update status - self
        this.setStatus(0);
        this.membership_list.get(Integer.parseInt(this.ip.substring(15, 17)) - 1).setStatus(0);

        // send leave message to neighbors
        Timestamp curr_timestamp = new Timestamp(System.currentTimeMillis());
        this.membership_list.get(Integer.parseInt(this.ip.substring(15, 17)) - 1).setTimestamp(curr_timestamp.getTime());
        for (int i = 0; i < neighbor_list.size(); i++) {
            String message = ip + "_" + membership_list.get(neighbor_list.get(i)).getIp() + "_" + curr_timestamp.getTime() + "_leave";
            int ret = -1;
            while (ret == -1) {
                ret = send_msg(this.socket, message, membership_list.get(neighbor_list.get(i)).getIp(), port, "leave");
            }
        }
        log("[INFO] " + server_id_in_log() + " - leave the group");
        this.neighbor_list = null;
    }

    // update the local membership_list
    // merge with the original
    public void updateMembershipList(ArrayList<Node> membership_list) {
        for (int i = 0; i < membership_list.size(); i++) {
            if (membership_list.get(i).getTimestamp() > this.membership_list.get(i).getTimestamp()) {
                this.membership_list.get(i).setId(membership_list.get(i).getId());
                this.membership_list.get(i).setTimestamp(membership_list.get(i).getTimestamp());
                this.membership_list.get(i).setStatus(membership_list.get(i).getStatus());
            }
        }
    }

    private void listMembershipList() {
        for (int i = 0; i < this.vm_list.size(); i++) {
            // VM01 - fa19-cs425-g46-01.cs.illinois.edu - idle/active
            String status = "Idle  ";
            if (this.membership_list.get(i).getStatus() == 1) {
                status = "Active";
            }
            System.out.println("VM" + String.format("%02d", i + 1) + " - " + this.vm_list.get(i) + " - " + status + "(Last update: " + MembershipListMaintainer.this.membership_list.get(i).getTimestamp() + ")");
        }
    }

    // start the service
    public void start() {
        // start the socket
        try {
            this.socket = new DatagramSocket(port);
        } catch (SocketException e) {
            log("[ERROR] " + server_id_in_log() + " - Socket creation failed!");
            e.printStackTrace();
        }

        // create threads
        CheckerThread checker = new CheckerThread();
        SenderThread sender = new SenderThread();
        ReceiverThread receiver = new ReceiverThread();

        // start execution
        checker.start();
        sender.start();
        receiver.start();

        // wait for threads to return
        try {
            checker.join();
            sender.join();
            receiver.join();
        } catch (InterruptedException e) {
            log("[ERROR] " + server_id_in_log() + " - Thread execution interrupted!");
            e.printStackTrace();
        }
        return;
    }

    // end the service
    public void end() {
        // close the socket
        this.socket.close();
        // close the file writer
        try {
            fw.close();
        } catch (IOException e) {
            System.err.println("[ERROR] File Writer closing failed!");
            e.printStackTrace();
        }
        return;
    }

    // list all virtual machines - assume that each vm has only one process
    public void listAllMembers() {
        System.out.println("All nodes in the membership list:");
        for (int i = 0; i < MembershipListMaintainer.this.membership_list.size(); i++) {
            // VM01 - fa19-cs425-g46-01.cs.illinois.edu (id: 1-0 ) - idle/active
            String status = "Idle  ";
            if (MembershipListMaintainer.this.membership_list.get(i).getStatus() == 1) {
                status = "Active";
            }
            System.out.println("VM" + String.format("%02d", i + 1) + " - " + MembershipListMaintainer.this.membership_list.get(i).getIp()
                    + " (id: " + String.format("%-16s", MembershipListMaintainer.this.membership_list.get(i).getId()) + " )"
                    + " - " + status + "(Last update: " + MembershipListMaintainer.this.membership_list.get(i).getTimestamp() + ")");
        }
    }

    // list all neighbors
    public void listAllNeighbors() {
        if (MembershipListMaintainer.this.status != 1) {
            return;
        }
        System.out.println("\nAll neighbors:");
        MembershipListMaintainer.this.updateNeighbor_list();
        for (int i : neighbor_list) {
            String status = "Idle  ";
            if (MembershipListMaintainer.this.membership_list.get(i).getStatus() == 1) {
                status = "Active";
            }
            System.out.println("VM" + String.format("%02d", i + 1) + " - " + MembershipListMaintainer.this.vm_list.get(i)
                    + " (id: " + String.format("%-16s", MembershipListMaintainer.this.membership_list.get(i).getId()) + " )"
                    + " - " + status + "(Last update: " + MembershipListMaintainer.this.membership_list.get(i).getTimestamp() + ")");
        }
    }

    // print info for the local server (self)
    public void printInfo() {
        // server id
        System.out.println("Server ID: " + MembershipListMaintainer.this.id);

        // server ip
        System.out.println("Server IP Address: " + MembershipListMaintainer.this.ip);

        // server status
        if (MembershipListMaintainer.this.status == 1) {
            System.out.println("Server Status: Active");
            Date d = new Date(MembershipListMaintainer.this.join_timestamp);
            System.out.println("Last Join Time: " + d.toString());
            Timestamp t = new Timestamp(System.currentTimeMillis());
            long duration = t.getTime() - MembershipListMaintainer.this.join_timestamp;
            long minutes = (duration / 1000) / 60;
            long seconds = (duration / 1000) % 60;
            System.out.format("Server Up Time: %d minutes %d seconds\n", minutes, seconds);
        } else {
            System.out.println("Server Status: Idle\n");
        }
    }

    // get all active members
    public ArrayList<Integer> getAllActiveMembers() {
        ArrayList<Integer> activeMembers = new ArrayList<Integer>();
        for (int i = 0; i < MembershipListMaintainer.this.membership_list.size(); i++) {
            if (MembershipListMaintainer.this.membership_list.get(i).getStatus() == 1) {
                activeMembers.add(i);
            }
        }
        return activeMembers;
    }

    /**
     * Checker thread scanning through the membership list periodically
     * Kick out any process that is idle for a pre-defined timeout value T-failure + T-cleanup
     */
    private class CheckerThread extends Thread {
        @Override
        public void run() {
            while (!shouldExit) {
                // check the thread every 0.5 seconds
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // if the node is idle, skip the checker
                if (MembershipListMaintainer.this.status != 1) {
                    continue;
                }
                // calculate the amount of active nodes, if it is less than 6, skip the checker
                int count = MembershipListMaintainer.this.getNumberOfActiveMembers();
                if (count <= 5) {
                    continue;
                }

                log("[INFO] " + server_id_in_log() + " - checked the membership list");
                Timestamp current_time = new Timestamp(System.currentTimeMillis());
                // update self
                if (MembershipListMaintainer.this.status != 1) {
                    continue;
                }
                MembershipListMaintainer.this.membership_list.get(Integer.parseInt(ip.substring(15, 17)) - 1).setTimestamp(current_time.getTime());
                for (Node n : MembershipListMaintainer.this.membership_list) {
                    if (n.getStatus() == 1) {
                        if (current_time.getTime() - n.getTimestamp() > MembershipListMaintainer.timeout_fail + MembershipListMaintainer.timeout_clean) {
                            // set status to be idle
                            n.setStatus(0);
                            n.setTimestamp(current_time.getTime());
                            log("[INFO] " + server_id_in_log() + " - marked " + n.getIp() + " - failed");
                        }
                    }
                }
                updateNeighbor_list();
            }
        }
    }

    /**
     * Sender thread keep sending heartbeat messages to its neighbors periodically
     */
    private class SenderThread extends Thread {
        @Override
        public void run() {
            // first get the neighbor ips
            while (!shouldExit) {
                // sending messages every 1 second
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                // if the node is idle, skip the sender
                if (MembershipListMaintainer.this.status != 1) {
                    continue;
                }
                int machine_id = Integer.parseInt(ip.substring(15, 17));
                Node current = membership_list.get(machine_id - 1);
                Timestamp curr_timestamp = new Timestamp(System.currentTimeMillis());
                // messages contain its own membership list, ip address, destination ip address, timestamp and "heartbeat" message type
                // construct a message including membership list
                int i;
                String message = "";
                for (i = 0; i < membership_list.size(); i++) {
                    message += membership_list.get(i).getIp() + "|"
                            + membership_list.get(i).getId() + "|"
                            + Long.toString(membership_list.get(i).getTimestamp()) + "|"
                            + Integer.toString(membership_list.get(i).getStatus()) + "_";
                }
                for (i = 0; i < neighbor_list.size(); i++) {
                    send_msg(socket, message + current.getIp() + "_"
                            + membership_list.get(neighbor_list.get(i)).getIp() + "_"
                            + curr_timestamp.getTime()
                            + "_heartbeat", membership_list.get(neighbor_list.get(i)).getIp(), port, "heartbeat");
                    log("[INFO] " + server_id_in_log() + "send heartbeat to " + membership_list.get(neighbor_list.get(i)).getIp());
                }
            }
        }
    }

    /**
     * Receiver thread watching on port 3001 for receiving messages from its neighbors
     */
    private class ReceiverThread extends Thread {
        @Override
        public void run() {
            while (!shouldExit) {
                if (MembershipListMaintainer.this.status != 1) {
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
                String[] message = info.split("_");
                // if the node is idle, skip the receiver
                if (MembershipListMaintainer.this.status != 1) {
                    continue;
                }
                switch (message[message.length - 1]) {
                    case "join":
                        // join message
                        int i = Integer.parseInt(message[0].substring(15, 17)) - 1;
                        membership_list.get(i).setId(message[1]);
                        membership_list.get(i).setTimestamp(Long.parseLong(message[2]));
                        membership_list.get(i).setStatus(1);
                        log("[INFO] " + server_id_in_log() + " - received " + message[0] + " - join");
                        updateNeighbor_list();
                        break;
                    case "leave":
                        // leave message
                        for (Node node : membership_list) {
                            if (node.getIp().equals(message[0])) {
                                node.setStatus(0); // mark as idle
                                long curr_timestamp = Long.parseLong(message[2]);
                                node.setTimestamp(curr_timestamp);
                                log("[INFO] " + server_id_in_log() + " - received from " + node.getIp() + " - leave");
                                break;
                            }
                        }
                        updateNeighbor_list(); // update neighbor list

                        break;
                    case "heartbeat":
                        // heartbeat message
                        String from_ip = message[message.length - 4];
                        long curr_timestamp = Long.parseLong(message[message.length - 2]);
                        MembershipListMaintainer.this.membership_list.get(Integer.parseInt(from_ip.substring(15, 17)) - 1).setStatus(1);
                        MembershipListMaintainer.this.membership_list.get(Integer.parseInt(from_ip.substring(15, 17)) - 1).setTimestamp(curr_timestamp);
                        log("[INFO] " + server_id_in_log() + " - received from " + from_ip + " - heartbeat");
                        ArrayList<Node> update_list = new ArrayList<Node>();
                        // convert message to membership list
                        for (int j = 0; j < message.length - 4; j++) {
                            String[] node = message[j].split("\\|");
                            Node nodeInfo = new Node();
                            nodeInfo.setIp(node[0]);
                            nodeInfo.setId(node[1]);
                            nodeInfo.setStatus(Integer.parseInt(node[3]));
                            nodeInfo.setTimestamp(Long.parseLong(node[2]));
                            update_list.add(nodeInfo);
                        }
                        updateMembershipList(update_list);
                        break;
                }
            }
        }
    }

    /**
     * Constructors
     */
    public MembershipListMaintainer(String id, String ip, ArrayList<String> vm_list) {
        this.id = id;
        this.ip = ip;
        this.vm_list = vm_list;
        this.neighbor_list = new ArrayList<Integer>();
        this.membership_list = new ArrayList<Node>();

        // for logging
        String currentDirectory = System.getProperty("user.dir");
        f = new File(currentDirectory.substring(0, currentDirectory.length() - 7) + "logs/vm" + id.substring(0, 2) + ".log");
        try {
            fw = new FileWriter(f);
        } catch (IOException e) {
            System.err.println("[ERROR] File Writer creation failed!");
            e.printStackTrace();
        }
    }

    public MembershipListMaintainer(String id, String ip, ArrayList<String> vm_list, ArrayList<Integer> neighbor_list) {
        this.id = id;
        this.ip = ip;
        this.vm_list = vm_list;
        this.neighbor_list = neighbor_list;
        this.membership_list = new ArrayList<Node>();

        // for logging
        String currentDirectory = System.getProperty("user.dir");
        f = new File(currentDirectory.substring(0, currentDirectory.length() - 7) + "logs/vm" + id.substring(0, 2) + ".log");
        try {
            fw = new FileWriter(f);
        } catch (IOException e) {
            System.err.println("[ERROR] File Writer creation failed!");
            e.printStackTrace();
        }
    }
}
