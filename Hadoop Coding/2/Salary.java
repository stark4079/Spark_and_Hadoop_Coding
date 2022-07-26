import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Salary {

  public static class SalaryMapper extends Mapper<Object,
                       Text,
                       IntWritable,
                       DoubleWritable> {
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      String tmp[] = line.split("\t");
      double salary = Double.parseDouble(tmp[3]);
      context.write(new IntWritable(1), new DoubleWritable(salary));
    }
  }

  public static class SalaryReducer
       extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {

      public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
       throws IOException, InterruptedException {
        double sum = 0;
        int count = 0;
        int key_tmp = key.get();
        for(DoubleWritable val: values){
          sum += val.get() * key_tmp;
          count += 1;
        }
        count *= key_tmp;
        context.write(new IntWritable(count), new DoubleWritable(sum/count));
  }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Salary");
    job.setJar("Salary.jar");
    job.setJarByClass(Salary.class);
    job.setMapperClass(SalaryMapper.class);
    job.setCombinerClass(SalaryReducer.class);
    job.setReducerClass(SalaryReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
