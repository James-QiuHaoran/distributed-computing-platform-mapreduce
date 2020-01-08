import java.util.HashSet;

/**
 * SDFS File Class
 * Store information about each file in the SDFS
 */
public class SDFSFile {
    private String SDFSFileName;
    private String localFilePath;
    private HashSet<Integer> replicas;
    private long timestamp;

    // support for directories
    private String SDFSDirectory;

    /**
     * Constructors
     */
    public SDFSFile() {
        this.SDFSFileName = null;
        this.localFilePath = null;
        this.replicas = null;
        this.timestamp = -1;
        this.SDFSDirectory = null;
    }

    public SDFSFile(String SDFSFileName, String localFilePath, HashSet<Integer> replicas, long timestamp) {
        this.SDFSFileName = SDFSFileName;
        this.localFilePath = localFilePath;
        this.replicas = replicas;
        this.timestamp = timestamp;

        // support for directories
        this.SDFSDirectory = SDFSFileName.substring(0, SDFSFileName.lastIndexOf("/") == -1 ? SDFSFileName.length() : SDFSFileName.lastIndexOf("/"));
    }

    /**
     * Getters and Setters
     */
    public String getSDFSFileName() {
        return SDFSFileName;
    }

    public void setSDFSFileName(String SDFSFileName) {
        this.SDFSFileName = SDFSFileName;
    }

    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public HashSet<Integer> getReplicas() {
        return replicas;
    }

    public void setReplicas(HashSet<Integer> replicas) {
        this.replicas = replicas;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String replicasToString() {
        String ret = "";
        for (Integer i : replicas) {
            ret += i + "=";
        }
        return ret;
    }

    public static HashSet<Integer> stringToReplicas(String replicaString) {
        HashSet<Integer> replicas = new HashSet<Integer>();
        String[] contents = replicaString.split("=");
        for (int i = 0; i < contents.length; i++) {
            replicas.add(Integer.parseInt(contents[i]));
        }
        return replicas;
    }

    public String getSDFSDirectory() {
        return SDFSDirectory;
    }

    public void setSDFSDirectory(String SDFSDirectory) {
        this.SDFSDirectory = SDFSDirectory;
    }

    // get the filename under the directory
    public String getFileName() {
        return this.SDFSFileName.substring(this.SDFSFileName.lastIndexOf("/") + 1);
    }
}