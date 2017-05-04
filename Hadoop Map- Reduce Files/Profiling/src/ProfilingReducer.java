import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfilingReducer extends
		Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		switch (key.toString()) {
		case "VendorID":
		case "RateCodeID":
			HashMap<Double, Integer> counts = new HashMap<Double, Integer>();
			for (DoubleWritable value : values) {
				if (counts.containsKey(value.get())) {
					counts.put(value.get(), counts.get(value.get()) + 1);
				} else {
					counts.put(value.get(), 1);
				}
			}

			int max = Integer.MIN_VALUE;
			double which_is_max = -1;
			for (Entry<Double, Integer> entry : counts.entrySet()) {
				context.write(new Text(key.toString() + "[" + entry.getKey()
						+ "]"), new DoubleWritable(entry.getValue()));
				if (max < entry.getValue()) {
					which_is_max = entry.getKey();
					max = entry.getValue();
				}
			}
			context.write(new Text("Mode[" + key.toString() + "]"),
					new DoubleWritable(which_is_max));
			break;
		case "Trip_distance":
			double total = 0;
			//let us find the average trip distance that people have travelled
			int total_trips = 0;
			for(DoubleWritable value1 : values ){
				total_trips ++ ;
				total = total + value1.get();
			}
			double average = total / total_trips;
			context.write(new Text(key.toString()) , new DoubleWritable(average));
			break;

		case "Fare_amount":
		case "Total_amount":
		case "Tip_amount":
		case "Tolls_amount":
		case "trip_distance":
		case "improvement_surcharge":
		case "Extra":
			double minf = Double.MAX_VALUE;
			double maxf = Double.MIN_VALUE;
			double mean = 0;
			int count = 0;
			for (DoubleWritable valuef : values) {
				count++;
				double current_val = valuef.get();
				if (current_val > maxf) {
					maxf = current_val;
				}
				if (current_val < minf) {
					minf = current_val;
				}
				mean += valuef.get();
			}
			mean /= count;
			context.write(new Text("Min[" + key.toString() + "]"),
					new DoubleWritable(minf));
			context.write(new Text("Max[" + key.toString() + "]"),
					new DoubleWritable(maxf));
			context.write(new Text("Mean[" + key.toString() + "]"),
					new DoubleWritable(mean));
			break;
		default:
			break;
		}
	}

}
