import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;



public class InpXmlFormatter extends  TextInputFormat {

  public static final String BEGIN_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";
  
  //Reads all records delimited by a specific tags (begin/end).
    
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit is, TaskAttemptContext tac)  {
        return new XmlRecordReader();       
    }
  public static class XmlRecordReader extends RecordReader<LongWritable,Text> {
	public  int counter = 0;
    private  byte[] beginTag;
    private  byte[] endTag;
    private  long begin;
    private  long end;
    private  FSDataInputStream fsdin;
    private  DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    

        
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            FileSplit fileSplit= (FileSplit) is;
            beginTag = tac.getConfiguration().get(BEGIN_TAG_KEY).getBytes("utf-8");
            endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");

            
                begin = fileSplit.getStart();
                end = begin + fileSplit.getLength();
                Path file = fileSplit.getPath();

                FileSystem fs = file.getFileSystem(tac.getConfiguration());
                fsdin = fs.open(fileSplit.getPath());
                fsdin.seek(begin);           
        }

        
        
        
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
        }

        
        public Text getCurrentValue() throws IOException, InterruptedException {
                   return value;
            
            

        }

        
        public float getProgress() throws IOException, InterruptedException {
            return (fsdin.getPos() - begin) / (float) (end - begin);
        }

        
        public void close() throws IOException {
            fsdin.close();
        }
        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
      int i = 0;
      while (true) {
        int b = fsdin.read();
        
        if (b == -1) return false;   //Reaches the EOF
        
        if (withinBlock) buffer.write(b); //Saved to the buffer

        
        if (b == match[i]) {    // Checked if equal
          i++;
          if (i >= match.length) return true;
        } else i = 0;
        
        if (!withinBlock && i == 0 && fsdin.getPos() >= end) return false;
      }
    }
        
        public boolean nextKeyValue() throws IOException, InterruptedException {
        	
            if (fsdin.getPos() < end) {
       if (readUntilMatch(beginTag, false)) {
         try {
           buffer.write(beginTag);
           if (readUntilMatch(endTag, true)) {
           	counter++;
           	System.out.println("$$$$$$$$"+counter);
           value.set(buffer.getData(), 0, buffer.getLength());
           key.set(fsdin.getPos());
                  return true;
           }
         } finally {
           buffer.reset();
         }
       }
     }
     return false;
       }
     
        

  }
  
    
}
