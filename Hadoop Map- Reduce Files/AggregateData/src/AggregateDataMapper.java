import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregateDataMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String input_line = value.toString();
		String[] input_line_cols = input_line.split(",");

		Double lonS = Math
				.round(Double.parseDouble(input_line_cols[5]) * 100.0) / 100.0;
		Double latS = Math
				.round(Double.parseDouble(input_line_cols[6]) * 100.0) / 100.0;
		Double lonD = Math
				.round(Double.parseDouble(input_line_cols[7]) * 100.0) / 100.0;
		Double latD = Math
				.round(Double.parseDouble(input_line_cols[8]) * 100.0) / 100.0;

		String keypair = lonS + "," + latS + "," + lonD + "," + latD;
		String valuepair = input_line_cols[24] + "---" + input_line_cols[25];

		context.write(new Text(keypair), new Text(valuepair));
	}
}
