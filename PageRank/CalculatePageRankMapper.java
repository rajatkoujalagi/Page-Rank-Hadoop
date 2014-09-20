import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class CalculatePageRankMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	protected void map(LongWritable key, Text values, Context context) throws IOException ,InterruptedException {
		
		long N=context.getConfiguration().getLong("N", 0);
		int iter=context.getConfiguration().getInt("iter", 0);
		
		double rank;
		String neighbour;
		int start;
		
		String value=values.toString().trim();
		String[] nodes=value.toString().trim().split("\t");
		
		String page = nodes[0].trim();
		if(iter>0){
			rank = Double.parseDouble(nodes[1]);
			neighbour=value.substring(nodes[0].length()+nodes[1].length()+1).trim();
			start=2;
		}else{
			rank = 1.0/N;
			neighbour=value.substring(nodes[0].length()).trim();
			start=1;
		}
		
		double newRank=(1-0.85)/N;
		context.write(new Text(page),new Text(newRank+"\t"+neighbour.trim()+"\t"+page.trim()));
		
		for(int i=start;i<nodes.length;i++){
			int noNeigh=nodes.length-start;
			newRank=0.85*rank/noNeigh;
			
			context.write(new Text(nodes[i]),new Text(String.valueOf(newRank)));
		}

	};

}
