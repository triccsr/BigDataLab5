package org.FeatureExtraction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.crypto.Data;
import java.io.DataInput;
import java.io.IOException;

public class FeatureExtraction {

  public static class FeatureExtractionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
      String[] nameAppearList = value.toString().split(";");
      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < nameAppearList.length - 1; i++) {
        String[] name1AndTimes = nameAppearList[i].split(",");
        String name1 = name1AndTimes[0];
        Long t1 = Long.parseLong(name1AndTimes[1]);
        for (int j = 0; j < nameAppearList.length - 1; j++) {
          if (j == i)
            continue;
          String[] name2AndTimes = nameAppearList[j].split(",");
          String name2 = name2AndTimes[0];
          Long t2 = Long.parseLong(name2AndTimes[1]);
          stringBuilder.append("<").append(name1).append(",").append(name2).append(">");
          context.write(new Text(stringBuilder.toString()), new LongWritable(t1 * t2));
          stringBuilder.setLength(0);
          stringBuilder.append("<").append(name2).append(",").append(name1).append(">");
          context.write(new Text(stringBuilder.toString()), new LongWritable(t1 * t2));
          stringBuilder.setLength(0);
        }
      }
    }
  }
  public static class FeatureExtractionReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
      long sum = 0L;
      for(LongWritable v : values){
        sum += v.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }

  public static void main(String[] args) {
    try {
      Job job = Job.getInstance(new Configuration(), "Feature Extraction");
      job.setJarByClass(FeatureExtraction.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(FeatureExtractionMapper.class);
      job.setReducerClass(FeatureExtractionReducer.class);
      job.setInputFormatClass(TextInputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LongWritable.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
