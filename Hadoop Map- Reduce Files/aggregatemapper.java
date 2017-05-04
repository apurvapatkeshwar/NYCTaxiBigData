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

public class aggregatemapper extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String input_line = value.toString();
        String[] input_line_cols = input_line.split(",");

        String one = input_line_cols[21];
        String two = input_line_cols[23];

        String keypair = one + "-->" + two;
        String valuepair = input_line_cols[25] + "---" + input_line_cols[26];

        context.write(new Text(keypair), new Text(valuepair));

    }
}

