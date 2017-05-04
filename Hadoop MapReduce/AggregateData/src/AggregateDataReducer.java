import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateDataReducer extends
		Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Double mean_citibike_time = 0.0;
		Double mean_taxi_time = 0.0;
		int count = 0;

		for (Text value : values) {
			String[] valuepart = value.toString().split("---");
			mean_citibike_time += Double.valueOf(valuepart[0]);
			mean_taxi_time += Double.valueOf(valuepart[1]);
			count++;
		}

		String output_value = key + "," + (mean_citibike_time / count) + ","
				+ (mean_taxi_time / count);

		context.write(null, new Text(output_value));
	}
}
