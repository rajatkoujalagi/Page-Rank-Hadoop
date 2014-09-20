import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankSortMapper extends Mapper<LongWritable, Text, DoubleWritable,Text> {
	public void map(LongWritable value, Text key, Context context ) throws IOException,InterruptedException  {
		long N=context.getConfiguration().getLong("N", 0);
		String splitter[]=key.toString().split("\t");
		DoubleWritable val=new DoubleWritable();
		
		
		if(Double.parseDouble(splitter[1])>=(5.0/N)){
			val.set(Double.parseDouble(splitter[1]));		
		    context.write(val, new Text(splitter[0])) ;
		}
	
	}

}
