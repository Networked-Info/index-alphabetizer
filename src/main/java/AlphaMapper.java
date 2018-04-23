import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AlphaMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		String term = value.toString().split("[\"]")[1];
		System.out.println(term);
//		if (term.charAt(0) >= 'a' && term.charAt(0) <= 'h') {
//			context.write(new Text(term), value);
//		}
//		if (term.charAt(0) >= 'i' && term.charAt(0) <= 'p') {
//			context.write(new Text(term), value);
//		}
		if (term.charAt(0) >= 'q' && term.charAt(0) <= 'z') {
			context.write(new Text(term), value);
		}
	}

}
