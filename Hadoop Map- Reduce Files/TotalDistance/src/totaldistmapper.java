import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.filecache.DistributedCache;



/**
 * Created by pranavchaphekar on 4/28/17.
 */
public class totaldistmapper extends Mapper<LongWritable, Text, LongWritable, Text> {


    HashMap<String, Double> map = new HashMap<>();

    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        super.setup(context);

        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            Scanner sc = new Scanner(new File("CitiBikeAvgTime.csv")).useDelimiter("\n");
//            sc.next(); //skip first line
            //creates a map of source + destination and time taken to reach in seconds
            while(sc.hasNext()){
                String line = sc.next();
                String[] line_split = line.split(",");
                String s = line_split[0].trim() + "+" + line_split[1].trim();
                map.put(s, Double.valueOf(line_split[2]));
            }
        }
        super.setup(context);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if(key.get() == 0 || key.get() == 1)
            return;

        String input_line = value.toString();

        String[] input_line_cols = input_line.split(",");

        String sd = input_line_cols[21].trim() + "+" + input_line_cols[23].trim();

        Double total_time = 0.0;


        for(String mapkey : map.keySet()){
            if(mapkey.equalsIgnoreCase(sd))
                total_time = map.get(mapkey) + Double.valueOf(input_line_cols[22]) + Double.valueOf(input_line_cols[24]);
        }

        if(total_time != 0.0){
            context.write(key, new Text(input_line + "," + total_time));
        }
    }
}
