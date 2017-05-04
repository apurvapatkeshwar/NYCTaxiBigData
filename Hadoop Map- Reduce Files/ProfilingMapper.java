import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfilingMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {
			/*
			Used for Data Profiling
			*/

	HashMap<String, Integer> cols = new HashMap<String, Integer>(); 

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 

		String[] keepcolumns = { "VendorID", "RateCodeID", "Passenger_count", "Trip_distance" ,"Fare_amount" , "Extra" , "MTA_tax" , "Tip_amount", "Tolls_amount" , "improvement_surcharge" , "Total_amount","Lpep_dropoff_datetime"};
		ArrayList<String> keep = new ArrayList<String>(
				Arrays.asList(keepcolumns));

		try {
			String line = value.toString();
			String[] values = line.split(",");

			if (key.get() == 0) {
				for (int i = 0; i < values.length; i++) {
					if (keep.contains(values[i])) {
						cols.put(values[i], i);
					}
				}
			} else {
				for (String s : cols.keySet()) {
					context.write(
							new Text(s),
							new DoubleWritable(Double.parseDouble(values[cols
									.get(s)])));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
