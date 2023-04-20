import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueIntegers {

  public static class UniqueIntegersMapper extends Mapper<Object, Text, IntWritable, NullWritable> {

    private IntWritable outKey = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] integers = value.toString().split(" ");
      for (String integer : integers) {
        try {
          outKey.set(Integer.parseInt(integer.trim()));
          context.write(outKey, NullWritable.get());
        } catch (NumberFormatException e) {
          // ignore non-integer values
        }
      }
    }
  }

  public static class UniqueIntegersReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "unique integers");
    job.setJarByClass(UniqueIntegers.class);
    job.setMapperClass(UniqueIntegersMapper.class);
    job.setCombinerClass(UniqueIntegersReducer.class);
    job.setReducerClass(UniqueIntegersReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
