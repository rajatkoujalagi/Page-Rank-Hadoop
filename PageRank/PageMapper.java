import java.io.IOException;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageMapper extends  Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		String xmlStr=value.toString();
		System.out.println(xmlStr.length());
		
		HashSet<String> set=new HashSet<String>();
		
		String[] sa=xmlStr.split("<title>")[1].split("</title>");
		String title=sa[0];
		
		Text titleKey = new Text(title.replace(' ', '_').trim());
		
		
		if(xmlStr.indexOf("<text")!=-1){
			String text="";
			try{
			text=xmlStr.split("<text.*>")[1].split("</text>")[0];}
			catch(Exception ex){
				System.out.println(xmlStr);
				System.exit(0);
			}
			Pattern pattern = Pattern.compile("\\[\\[.+?\\]\\]");
			Matcher matcher = pattern.matcher(text);
			while(matcher.find()){
				String parsed=matcher.group();
				String val="";
				int pipe=parsed.indexOf("|");
				if(pipe==-1){
					val=parsed.substring(2, parsed.length()-2);
				}else{
					val=parsed.substring(2, pipe);
				}
				
				String neigh=val.replace(' ', '_').trim();
				if(!neigh.equals(titleKey.toString()) && !set.contains(neigh)){
					
					context.write(new Text(neigh),titleKey);
					set.add(neigh);
				}
		}
			
		}
		context.write(titleKey,new Text("*"));
	};
	

}
