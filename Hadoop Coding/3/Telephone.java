import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import java.time.LocalDateTime;
import java.time.Duration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Telephone {

  public static class TelephoneMapper extends Mapper<Object,
                       Text,
                       Text,
                       LongWritable> {
    public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
      String line = value.toString();
      String tmp[] = line.split("[|]");
      String FromPhone = tmp[0];
      int STDflag = Integer.parseInt(tmp[4]);
      long CallingTime = 0;
      if(STDflag == 1 ){
        CallingTime = cacl_calling_time(tmp[2], tmp[3]);
      }
      context.write(new Text(FromPhone), new LongWritable(CallingTime));
    }
    long cacl_calling_time(String from, String to){
      // First is date, second is time
      String from_data[] = from.split(" ");
      String to_data[] = to.split(" ");
      // contains data about year, month, day in order from left to right
      String from_tmp[] = from_data[0].split("-");
      String to_tmp[] = to_data[0].split("-");
      int from_date[] = new int[3];
      int to_date[] = new int[3];
      for(int i =0; i < 3; i++){
        from_date[i] = Integer.parseInt(from_tmp[i]);
        to_date[i] = Integer.parseInt(to_tmp[i]);
      }
      // contains data about hour, minute, second inoder from left to right
      String from_tmp1[] = from_data[1].split(":");
      String to_tmp1[] = to_data[1].split(":");

      int from_time[] = new int[3];
      int to_time[] = new int[3];
      for(int i = 0; i < 3; i++){
        from_time[i] = Integer.parseInt(from_tmp1[i]);
        to_time[i] = Integer.parseInt(to_tmp1[i]);
      }

      LocalDateTime Start = LocalDateTime.of(from_date[0], from_date[1], from_date[2],
                                             from_time[0], from_time[1], from_time[2]);
      LocalDateTime End = LocalDateTime.of(to_date[0], to_date[1], to_date[2],
                                           to_time[0], to_time[1], to_time[2]);

      long total_time = Duration.between(Start, End).toMinutes();

      return total_time;
    }
  }


  public static class TelephoneReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {

      public void reduce(Text key, Iterable<LongWritable> values, Context context)
       throws IOException, InterruptedException {
        long total_time = 0;
        for(LongWritable val: values){
          total_time += val.get();
        }
        if(total_time > 60){
          context.write(key, new LongWritable(total_time));
        }
      }
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Telephone");
    job.setJar("Telephone.jar");
    job.setJarByClass(Telephone.class);
    job.setMapperClass(TelephoneMapper.class);
    job.setCombinerClass(TelephoneReducer.class);
    job.setReducerClass(TelephoneReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
