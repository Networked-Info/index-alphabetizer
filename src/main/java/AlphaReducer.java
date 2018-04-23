import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AlphaReducer extends Reducer<Text, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> out;

	 public void setup(Context context) {
		out = new MultipleOutputs<Text,NullWritable>(context);   
	 }

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		char first = key.toString().charAt(0);
//		if (first >= 'a' && first <= 'h') {
//		if (first >= 'i' && first <= 'p') {
		if (first >= 'q' && first <= 'z') {
			char second;
			if (key.toString().length() == 1) {
				second = first;
			} else {
				second = key.toString().charAt(1);
			}
			
			String outFile = first + "" + second;
			for (Text t : values) {
				out.write(t, NullWritable.get(), outFile);
			}
			
		}
	}
	
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 out.close();
	 }	

}
