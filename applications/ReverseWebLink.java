import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReverseWebLink {

    public static class ReverseLinksMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splited = line.split(",");
            context.write(new Text(splited[1]), new Text(splited[0]));
        }
    }

    public static class GatherLinksReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String result = "[ ";
            for (Text val : values) {
                result += val.toString() + " ";
            }
            context.write(key, new Text(result + "]"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: ReverseWebLink <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "reverse web link graph");
        job.setJarByClass(ReverseWebLink.class);
        job.setMapperClass(ReverseLinksMapper.class);
        job.setCombinerClass(GatherLinksReducer.class);
        job.setReducerClass(GatherLinksReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
