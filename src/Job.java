public class Job {
    // job type - // 1 for map and 2 for reduce
    private int jobType;

    // job executable file
    private String jobExe;

    // number of tasks
    private int numTasks;

    // prefix for intermediate files
    private String sdfs_intermediate_filename_prefix;

    // directory for input files - only for map job
    private String sdfs_src_directory;

    // out file name - only for reduce job
    private String sdfs_dest_filename;

    // 1 for delete intermediate files and 0 for not - only for reduce job
    private int deleteInput;

    public Job(int jobType, String jobExe, int numTasks, String sdfs_intermediate_filename_prefix, String sdfs_src_directory) {
        this.jobType = jobType;
        this.jobExe = jobExe;
        this.numTasks = numTasks;
        this.sdfs_intermediate_filename_prefix = sdfs_intermediate_filename_prefix;
        this.sdfs_src_directory = sdfs_src_directory;
    }

    public Job(int jobType, String jobExe, int numTasks, String sdfs_intermediate_filename_prefix, String sdfs_dest_filename, int deleteInput) {
        this.jobType = jobType;
        this.jobExe = jobExe;
        this.numTasks = numTasks;
        this.sdfs_intermediate_filename_prefix = sdfs_intermediate_filename_prefix;
        this.sdfs_dest_filename = sdfs_dest_filename;
        this.deleteInput = deleteInput;
    }

    public int getJobType() {
        return jobType;
    }

    public String getJobExe() {
        return jobExe;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public String getSdfs_intermediate_filename_prefix() {
        return sdfs_intermediate_filename_prefix;
    }

    public String getSdfs_src_directory() {
        return sdfs_src_directory;
    }

    public String getSdfs_dest_directory() {
        return sdfs_dest_filename;
    }

    public int getDeleteInput() {
        return deleteInput;
    }

}
