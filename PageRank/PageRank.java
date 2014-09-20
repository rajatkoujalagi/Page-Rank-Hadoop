//package PageRank;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PageRank {

static String tmpFolder="output";
static String outputFolder="output";
static long N;
static String bucket;

public PageRank(String buck) {

bucket=buck;
tmpFolder=buck+"/tmp/job";
outputFolder=buck+"/results/job";
}

public void task0(String input) throws IOException{
Configuration conf=new Configuration();
conf.set("xmlinput.start", "<page>");
               conf.set("xmlinput.end", "</page>");
               conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
Job job=new Job(conf,"PageRank");
job.setJarByClass(PageRank.class);
job.setMapperClass(PageMapper.class);
        job.setReducerClass(PageReducer.class);
        
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        
        job.setInputFormatClass(InpXmlFormatter.class);
        
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        try {
job.waitForCompletion(true);
} catch (ClassNotFoundException | InterruptedException e) {

e.printStackTrace();
}
        
        Path path;
path=new Path(outputFolder);
FileSystem fsystem=path.getFileSystem(conf);
FileUtil.copyMerge(fsystem, path, fsystem, new Path(path.getParent().toString(),"PageRank.outlink_tmp.out"), true, conf, null);
     
        
}
public void task1() throws IOException{
Configuration conf=new Configuration();
conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
Job job=new Job(conf,"PageRankReverse");
job.setJarByClass(PageRank.class);
job.setMapperClass(PageRankRevMapper.class);
        job.setReducerClass(PageReducer.class);
        
        FileInputFormat.setInputPaths(job, new Path(bucket+"/results/PageRank.outlink_tmp.out"));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        
        job.setInputFormatClass(TextInputFormat.class);
        
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        try {
job.waitForCompletion(true);
} catch (ClassNotFoundException | InterruptedException e) {

e.printStackTrace();
}
        
        Path path;
path=new Path(outputFolder);
FileSystem fsystem=path.getFileSystem(conf);
FileUtil.copyMerge(fsystem, path, fsystem, new Path(path.getParent().toString(),"PageRank.outlink.out"), true, conf, null);
fsystem.delete(new Path(path.getParent().toString(),"PageRank.outlink_tmp.out"), false);
        
}
public void task2() throws IOException{
Configuration conf=new Configuration();
conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
Job job=new Job(conf,"CalculateN");
job.setJarByClass(PageRank.class);
job.setMapperClass(CalculateNMapper.class);
        job.setReducerClass(CalculateNReducer.class);
        
        FileInputFormat.setInputPaths(job, bucket+"/results/PageRank.outlink.out");
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        
        job.setInputFormatClass(TextInputFormat.class);
        
        
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        try {
job.waitForCompletion(true);
} catch (ClassNotFoundException | InterruptedException e) {

e.printStackTrace();
}
        
        Path path;
path=new Path(outputFolder);
FileSystem fsystem=path.getFileSystem(conf);
FileUtil.copyMerge(fsystem, path, fsystem, new Path(path.getParent(),"PageRank.n.out"), true, conf, null);
}

public void task3(int iter) throws IOException{
Configuration conf=new Configuration();
conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
if(iter==0)
{
String uriStr = bucket+"/results/"; 
URI uri = URI.create(uriStr); 
FileSystem fs = FileSystem.get(uri,new Configuration());
Path pt=new Path(bucket+"/results/PageRank.n.out");
       BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
       String line=br.readLine();
br.close();
       fs.close();
       N=Long.parseLong(line.substring(2));
}	
conf.setLong("N", N);
conf.setInt("iter", iter);
Job job=new Job(conf,"Calculate PR");
job.setJarByClass(PageRank.class);
job.setMapperClass(CalculatePageRankMapper.class);
        job.setReducerClass(CalculatePageRankReducer.class);
        if(iter==0)
        FileInputFormat.setInputPaths(job, bucket+"/results/PageRank.outlink.out");
        else
        FileInputFormat.setInputPaths(job, bucket+"/results/PageRank.iter"+iter+"_tmp.out");
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        try {
job.waitForCompletion(true);
} catch (ClassNotFoundException | InterruptedException e) {

e.printStackTrace();
}
        
        Path path;
path=new Path(outputFolder);
FileSystem fsystem=path.getFileSystem(conf);
FileUtil.copyMerge(fsystem, path, fsystem, new Path(path.getParent(),"PageRank.iter"+(iter+1)+"_tmp.out"), true, conf, null);
if(iter>0)
fsystem.delete(new Path(path.getParent(),"PageRank.iter"+(iter)+"_tmp.out"), false);
   	}
public void task4(int iter) throws IOException{
Configuration conf=new Configuration();
conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
conf.setLong("N", N);
conf.setInt("iter", iter);
Job job=new Job(conf,"Sort PR");
job.setJarByClass(PageRank.class);
job.setMapperClass(PageRankSortMapper.class);
        job.setReducerClass(PageRankSortReducer.class);
        
        FileInputFormat.setInputPaths(job, bucket+"/results/PageRank.iter"+iter+"_tmp.out");
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);
        job.setSortComparatorClass(SortKeyComparator.class);
    
       
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        
        try {
job.waitForCompletion(true);
} catch (ClassNotFoundException | InterruptedException e) {

e.printStackTrace();
}
        
        Path path;
path=new Path(outputFolder);
FileSystem fsystem=path.getFileSystem(conf);
FileUtil.copyMerge(fsystem, path, fsystem, new Path(path.getParent(),"PageRank.iter"+(iter)+".out"), true, conf, null);
if(iter==8)
fsystem.delete(new Path(path.getParent(),"PageRank.iter"+(iter)+"_tmp.out"), false);
   	}

public static void main(String[] args) throws IOException {
String input=args[0].trim();
PageRank pg=new PageRank(args[1].trim());
pg.task0(input);
pg.task1();
pg.task2();
       
for(int i=0;i<8;i++){
	pg.task3(i);
	if(i==0 || i==7)
	pg.task4(i+1);
  }
}



}