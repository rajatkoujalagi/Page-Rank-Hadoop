import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CalculatePageRankReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, java.lang.Iterable<Text> valueList, Context context) throws IOException ,InterruptedException {
		
		boolean writeFlag=false;
		double rank=0.0;
		
		Iterator<Text> values=valueList.iterator();
		
		String neighbour="";
		
		//Iterates through the values
		while(values.hasNext()){
		    String value=values.next().toString().trim();
		    String[] nodes=value.split("\t");
		    if(nodes[nodes.length-1].trim().equals(key.toString().trim()))
		    	writeFlag=true;
	    	
	    	rank+=Double.parseDouble(nodes[0].trim());
	    	
	    	if(nodes.length>1){
	    		neighbour=value.trim().substring(nodes[0].length(),value.length()-key.toString().length());
	    	}
		    
	    }
		if(writeFlag)
			context.write(new Text(key+"\t"+rank),new Text(neighbour.trim()));
	}
}
