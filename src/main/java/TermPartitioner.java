import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TermPartitioner extends Partitioner<Text, Text>implements Configurable {
	private Configuration conf = null;

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		System.out.println("Key: " + key.toString());
		char sortBy = 'x';
		if (key.toString().length() == 1) {
			sortBy = key.toString().charAt(0);
		}
		else if (key.toString().length() == 2) {
			sortBy = key.toString().charAt(1);
		} else {
			sortBy = key.toString().charAt(2);
		}
		
		int num = 0;
		if (sortBy >= 'a' && sortBy <= 'g') {
			num = 0;
		} else if (sortBy >= 'h' && sortBy <= 'm') {
			num = 1;
		} else if (sortBy >= 'n' && sortBy <= 's') {
			num = 2;
		} else {
			num = 3;
		}
		return Math.abs(num % numPartitions);
//		return Math.abs(sortBy.hashCode()) % numPartitions);
	}
}