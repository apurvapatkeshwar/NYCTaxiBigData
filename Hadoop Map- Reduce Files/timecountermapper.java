import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.filecache.DistributedCache;

/**
 * Created by pranavchaphekar on 4/27/17.
 */

public class timecountermapper extends Mapper<LongWritable, Text, Text,LongWritable > {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String input_line = value.toString();
        String[] input_line_cols = input_line.split(",");

        String one = input_line_cols[1];
        String[] two = one.split(" ");
        String[] three = two[1].split(":");

        context.write(three[0], 1);

    }
}
