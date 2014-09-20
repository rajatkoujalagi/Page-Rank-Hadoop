import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class PageReducer extends Reducer<Text, Text, Text, Text> {
	
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
		System.out.println(key.toString());
		boolean writeFlag=false;
		StringBuffer valueStr = new StringBuffer("");
		Iterator<Text> iterVal=values.iterator();
	
	    while (iterVal.hasNext()){
	    	String temp=iterVal.next().toString().trim();
	    	System.out.println(key.toString()+" "+temp);
	    	if(temp.equals("*")){
	    		writeFlag=true;
	    	}else{
	    		valueStr.append(temp);
			    valueStr.append("\t");
	    	}
	    }
	    if(writeFlag)
	    	context.write(key, new Text(valueStr.toString().trim()));
	};
	
	

}
