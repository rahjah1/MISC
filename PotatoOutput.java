import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.mapreduce.RecordWriter;
public class PotatoOutput extends FileOutputFormat<Text, IntWritable>
{
  private static String WORD = "";
	
  protected static class PotatoRecordWriter extends RecordWriter<Text, IntWritable> {
    private static final String utf8 = "UTF-8";

    private DataOutputStream out;

    public PotatoRecordWriter(DataOutputStream out) throws IOException 
    {
      this.out = out;
    }

    private void writeValue(IntWritable o) throws IOException {
        out.write(o.toString().getBytes(utf8));
	out.writeBytes(">\n");
    }

    private void writeKey(String o) throws IOException {
      out.writeBytes("<" + o + ", ");
    }

    @Override
    public void write(Text key, IntWritable value) throws IOException 
    {
	String keyString = key.toString();
        if(!WORD.equals(keyString.substring(0, keyString.indexOf(" "))))
	{
		out.writeBytes("\n");
		WORD = keyString.substring(0, keyString.indexOf(" "));
		writeWord(WORD);
	}

	writeKey(keyString.substring(WORD.length() + 1, keyString.length()));
	writeValue(value);
    }

    private void writeWord(String o) throws IOException
    {
	out.writeBytes(o+"\n");
    }

    @Override
    public void close(TaskAttemptContext arg0) throws IOException 
    {
        // even if writeBytes() fails, make sure we close the stream
        out.close();
    }
  }

  @Override
  public org.apache.hadoop.mapreduce.RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext arg0) throws IOException
  {	
    Path file = FileOutputFormat.getOutputPath(arg0);
    Path fullPath = new Path(file, "result.txt");
    FileSystem fs = file.getFileSystem(arg0.getConfiguration());
    FSDataOutputStream fileOut = fs.create(fullPath, arg0);
    return new PotatoRecordWriter(fileOut);
  }
}
