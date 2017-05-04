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

public class TaxiDistMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    //Calculates the haversine distance (nearest)
    public static double haversine(
            double lat1, double lng1, double lat2, double lng2) {
        int r = 6371; // average radius of the earth in km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lng2 - lng1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                        * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double d = r * c;
        return d;
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String input_line = value.toString();
        String[] input_line_cols = input_line.split(",");

        String pickup_time = input_line_cols[1];
        String drop_off_time = input_line_cols[2];

        String pickup[] = pickup_time.split(" ");
        String dropoff[] = drop_off_time.split(" ");

        if(pickup[0].equalsIgnoreCase(dropoff[0])){
            String actual_time_pickup[] = pickup[1].split(":");
            String actual_time_dropoff[] = dropoff[1].split(":");
            int time = (Integer.valueOf(actual_time_dropoff[0]) - Integer.valueOf(actual_time_pickup[0])) * 3600 +
                    (Integer.valueOf(actual_time_dropoff[1]) - Integer.valueOf(actual_time_pickup[1])) * 60 +
                    (Integer.valueOf(actual_time_dropoff[2]) - Integer.valueOf(actual_time_pickup[2])) ;

            String output_value = input_line + "," + time;
            context.write(key, new Text(output_value));

        }
    }
}
