import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(),
				new Driver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.out
					.printf("Two parameters are required for DriverFormatMultiOutput- <input dir> <output dir>\n");
			return -1;
		}	
		
		Configuration conf = new Configuration();

		Job j = Job.getInstance(conf, "alpha");
		j.setPartitionerClass(TermPartitioner.class);	
		j.setNumReduceTasks(4);
		j.setJarByClass(Driver.class);
		LazyOutputFormat.setOutputFormatClass(j, TextOutputFormat.class);
		FileInputFormat.setInputPaths(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));

		j.setMapperClass(AlphaMapper.class);
		j.setReducerClass(AlphaReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(Text.class);

		boolean success = j.waitForCompletion(true);
		return success ? 0 : 1;
	}

}
