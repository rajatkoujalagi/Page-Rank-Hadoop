import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

class SortKeyComparator extends WritableComparator {
    
	/**			Sorts in the descending order **/
public int compare(WritableComparable a, WritableComparable b) {
	        DoubleWritable dw1 = (DoubleWritable) a;
	        DoubleWritable dw2 = (DoubleWritable) b;
	        if(dw1.get() < dw2.get()) {
	            return 1;
	        }else if(dw1.get() > dw2.get()) {
	            return -1;
	        }else {
	            return 0;
	        }
	    }
	
    protected SortKeyComparator() {
        super(DoubleWritable.class, true);
    }

     
}

public class PageRankSortReducer extends Reducer< DoubleWritable, Text, Text, DoubleWritable> {
	
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException  
	{
		for (Text val : values )
		{
			context.write(val,key);
		}
	}

}
