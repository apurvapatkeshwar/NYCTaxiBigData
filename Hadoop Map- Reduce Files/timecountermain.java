import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class timecountermain {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DataCleaner <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(timecountermain.class);
        job.setJobName("Total Taxi time");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(timecountermapper.class);
        job.setReducerClass(timecounterreducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}