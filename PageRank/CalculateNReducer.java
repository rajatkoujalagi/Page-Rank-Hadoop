import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CalculateNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	protected void reduce(Text key, java.lang.Iterable<IntWritable> value,Context context) throws IOException ,InterruptedException {
		Iterator<IntWritable> values=value.iterator();
		long sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		context.write(new Text(key.toString()+sum), null);
	};
}
