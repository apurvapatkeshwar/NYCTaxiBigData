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

public class LocMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    class Location {
        Double latitude;
        Double longitude;
        String loc_name;

        public Location(String loc_name, Double latitude, Double longitude){
            this.loc_name = loc_name;
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    /*
    A particular entry consists of information about the location
    attributes - area_name, latitude, longitude
     */
    ArrayList<Location> loc = new ArrayList<>();


    @Override
    public void setup(Context context) throws IOException,InterruptedException{
        super.setup(context);

        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {

            Scanner sc = new Scanner(new File("Latitudes_Longitudes.csv")).useDelimiter("\n");
            sc.next(); //skip first line
            while(sc.hasNext()){
                String line = sc.next();
                String[] line_split = line.split(",");
                loc.add(new Location(line_split[0],Double.valueOf(line_split[1]), Double.valueOf(line_split[2])));
            }
        }
        super.setup(context);
    }

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

        if(key.get() == 0 || key.get() == 1)
            return;

        String input_line = value.toString();
        String[] input_line_cols = input_line.split(",");

        double pickup_long = Double.valueOf(input_line_cols[5]);
        double pickup_lat = Double.valueOf(input_line_cols[6]);
        double dropoff_long = Double.valueOf(input_line_cols[7]);
        double dropoff_lat = Double.valueOf(input_line_cols[8]);

        double min_distance_pickup = Double.MAX_VALUE;
        double min_distance_dropoff = Double.MAX_VALUE;


        String closest_loc_pickup = loc.get(0).loc_name;
        String closest_loc_dropoff = loc.get(0).loc_name;


        for(Location location : loc ){
            double haversine_distance_pickup = haversine(pickup_lat, pickup_long, location.latitude,location.longitude);
            double haversine_distance_dropoff = haversine(dropoff_lat,dropoff_long, location.latitude,location.longitude);
            if(haversine_distance_pickup < min_distance_pickup){
                closest_loc_pickup = location.loc_name;
                min_distance_pickup = haversine_distance_pickup;
            }
            if(haversine_distance_dropoff < min_distance_dropoff){
                closest_loc_dropoff = location.loc_name;
                min_distance_dropoff = haversine_distance_dropoff;
            }
        }
        String output_value = input_line + "," + closest_loc_pickup + "," + (min_distance_pickup * 720) + "," + closest_loc_dropoff + ","  + (min_distance_dropoff * 720);

        context.write(key, new Text(output_value));

    }
}
