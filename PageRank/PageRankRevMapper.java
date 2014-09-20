import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankRevMapper extends  Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		String fileStr=value.toString().trim();
		
		String[] titles=fileStr.split("\t");
		
		for(int i=1;i<titles.length;i++){
			context.write(new Text(titles[i]),new Text(titles[0]));
			context.write(new Text(titles[i]),new Text("*"));
		}
		context.write(new Text(titles[0]), new Text("*"));
	};
	

}
