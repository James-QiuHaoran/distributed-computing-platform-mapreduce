/**
 * Node Class
 * Storing information about each node in the membership list
 */
public class Node {
    private String ip;
    private String id;
    private long timestamp;
    private int status;

    /**
     * Constructors
     */
    public Node() {
    }

    public Node(String ip, String id, long timestamp, int status) {
        this.ip = ip;                // node ip
        this.id = id;                // node id - vm_id + timestamp_joining
        this.timestamp = timestamp;  // timestamp of the last heartbeat
        this.status = status;        // 0 for idle | 1 for active | 2 for leave
    }

    /**
     * Getter and Setters
     */
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}