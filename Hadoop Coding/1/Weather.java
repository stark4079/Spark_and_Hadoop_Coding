import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Weather {

  public static class WeatherMapper extends Mapper<Object,
                       Text,
                       Text,
                       FloatWritable> {
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      String tmp = line.substring(6, 14);
      String date = tmp.substring(6, 8) + '-' + tmp.substring(4, 6) +'-' + tmp.substring(0, 4);
      float maxtmp = Float.parseFloat( line.substring(39, 45));
      float mintmp = Float.parseFloat(line.substring(47, 53));
      if (mintmp < 10.0){
        context.write(new Text( date + " Cold Day"), new FloatWritable(mintmp));
      }
      else if (maxtmp > 40.0){
        context.write(new Text( date + " Hot Day"), new FloatWritable(maxtmp));
      }
    }
  }
  public static class WeatherReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
      public void reduce(Text key, FloatWritable values, Context context)
       throws IOException, InterruptedException {
        context.write(key, new FloatWritable(values.get()));
  }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Weather");
    job.setJar("Weather.jar");
    job.setJarByClass(Weather.class);
    job.setMapperClass(WeatherMapper.class);
    job.setCombinerClass(WeatherReducer.class);
    job.setReducerClass(WeatherReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
