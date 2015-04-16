import java.io.IOException;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.StringTokenizer;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.conf.Configurable;
public class CountingIndexer 
{
  public static class KeyComparator extends WritableComparator
  {
	public KeyComparator()
	{
		super(ComboKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		ComboKey t1 = (ComboKey) w1;
		ComboKey t2 = (ComboKey) w2;
		return t1.compareTo(t2);
	}
  }

  public static class GroupComparator extends WritableComparator
  {
	public GroupComparator()
	{
		super(ComboKey.class, true);
	}
	@Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
                ComboKey t1 = (ComboKey) w1;
                ComboKey t2 = (ComboKey) w2;
                return t1.word.compareTo(t2.word);
        }	
  }
  public class PotatoPartitioner extends Partitioner<ComboKey, IntWritable>
  {
	@Override
	public int getPartition(ComboKey key, IntWritable value, int numPartitions)
	{
		String test = key.word;
		int num = 0;
		for(int i = 0; i < test.length(); i++)
		{
			num += test.charAt(i);
		}
		
		//return (key.word.hashCode()) % numPartitions;
		return num % numPartitions;
	}
  }

  public static class TokenizerMapper extends Mapper<Object, Text, ComboKey, IntWritable>
  {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
      FileSplit filesplit = (FileSplit)context.getInputSplit();
      String fileName = new String();
      fileName = filesplit.getPath().getName(); //Get filenamei
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) 
      {
	String subject = itr.nextToken();
	for(int i = 0; i < subject.length(); i++)
	{
		if(!Character.isLetter(subject.charAt(i)))
		{
			subject = subject.substring(0, i) + "" + subject.substring(i+1);
			i--;
		}
	}
	subject = subject.toLowerCase();
	subject.trim();
	if(!subject.isEmpty())
	{
		context.write(new ComboKey(fileName, subject, one), one);
	}
      }
    }
  }

  public static class IntSumReducer extends Reducer<ComboKey,IntWritable,ComboKey,IntWritable> 
  {
    private IntWritable result = new IntWritable();
    public void reduce(ComboKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
    {
      int sum = 0;
      for (IntWritable val : values) 
      {
        sum += val.get();
      }
      result.set(sum);
      key.sum = result;
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Counting Indexer");
    job.setOutputFormatClass(PotatoOutput.class);
    job.setJarByClass(CountingIndexer.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);

    job.setSortComparatorClass(KeyComparator.class);
    job.setPartitionerClass(PotatoPartitioner.class);
    job.setGroupingComparatorClass(GroupComparator.class);
    job.setMapOutputKeyClass(ComboKey.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(ComboKey.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
