import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by pranavchaphekar on 4/29/17.
 */
public class timecounterreducer extends Reducer<Text, LongWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

            int count = 0;
        for(Text value : values){
            ++ count;
        }

        context.write(key, new Text(count));
    }
}