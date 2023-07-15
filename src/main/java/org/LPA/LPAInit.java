package org.LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LPAInit {
  public static class LPAInitMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
      List<String> inEdges =
              Arrays.stream(value.toString().split("[\\[\\]|]")).filter(s -> s.length() > 0).collect(Collectors.toList());
      for (String innodes : inEdges) {
//        System.out.println(innodes);
        String[] nodeAndWeight = innodes.split(",");
        String node_name = nodeAndWeight[0];
        String weight = nodeAndWeight[1];
        context.write(new Text(node_name), new Text(key + "," + weight));
      }
    }
  }

  public static class LPAInitReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
      StringBuilder valueBuilder = new StringBuilder();
      for (Text v : values) {
        valueBuilder.append(v.toString());
        valueBuilder.append(";");
      }
      StringBuilder keyBuilder = new StringBuilder();
      keyBuilder.append(key.toString()).append("[").append(key.toString()).append("]");
      context.write(new Text(keyBuilder.toString()), new Text(valueBuilder.toString()));
    }
  }

//  public static void run(String[] args) {
//  }
//
//  public static void main(String[] args) {
//    run(args);
//  }
}
