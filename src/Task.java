import java.util.ArrayList;
import java.text.DecimalFormat;

public class Task {
    private String taskType;
    private int taskID;
    private int nodeID;
    private ArrayList<String> inputFiles;
    private double progress;
    private String outputFile;
    private boolean committed;

    private DecimalFormat df = new DecimalFormat("#.##");

    public Task(String taskType, int taskID, int nodeID, ArrayList<String> inputFiles, double progress) {
        this.taskType = taskType;
        this.taskID = taskID;
        this.nodeID = nodeID;
        this.inputFiles = inputFiles;
        this.progress = progress;
        this.outputFile = null;
        this.committed = false;
    }

    public int getTaskID() {
        return taskID;
    }

    public int getNodeID() {
        return nodeID;
    }

    public ArrayList<String> getInputFiles() {
        return inputFiles;
    }

    public double getProgress() {
        return progress;
    }

    public void setProgress(double progress) {
        this.progress = progress;
    }

    public void printInfo() {
        System.out.println(this.taskType + " Task #" + this.taskID + " - " + (this.nodeID == -1 ? "Inactive" : "Active"));
        System.out.println("Worker ID: " + (this.nodeID == -1 ? "N/A" : this.nodeID + 1));
        System.out.println("Input Files: " + this.inputFiles);
        System.out.println("Progress: " + (this.nodeID == -1 ? "N/A" : df.format(this.progress * 100) + "%") + "\n");
    }

    public void printBriefInfo() {
        System.out.println("Task #" + this.taskID + " - Worker (" + (this.nodeID == -1 ? "N/A" : this.nodeID + 1) + ") - Progress (" + (this.nodeID == -1 ? "N/A" : df.format(this.progress * 100) + "%") + ")");
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    public String getOutputFile() {
        return this.outputFile;
    }

    public void setNodeID(int nodeID) {
        this.nodeID = nodeID;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }
}
