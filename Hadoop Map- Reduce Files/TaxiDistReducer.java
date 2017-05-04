import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by pranavchaphekar on 4/29/17.
 */
public class TaxiDistReducer extends Reducer<LongWritable, Text, Text, Text> {

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for(Text value : values){
            context.write(null, value);
        }
    }
}