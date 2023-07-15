package org.LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.bind.ValidationEvent;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class LPARebuild {


  public static class LPARebuildMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
      String[] nameAndLabel = key.toString().split("[\\[\\]]");
      String key_name = nameAndLabel[0];
      String key_label = nameAndLabel[1];
      String[] innodes = value.toString().split(";");
      for (String innode : innodes) {
        String[] nodeAndWeight = innode.split(",");
        String node_name = nodeAndWeight[0];
        String weight = nodeAndWeight[1];
        context.write(new Text(node_name), new Text(key_name + "," + weight));
      }
      context.write(new Text(key_name), new Text(key_label));
    }
  }

  public static class LPARebuildReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
      StringBuilder valueBuilder = new StringBuilder();
      StringBuilder keyBuilder = new StringBuilder();
      keyBuilder.append(key.toString());
      for (Text v : values) {
        String v_str = v.toString();
        if (v_str.contains(",")) {
          valueBuilder.append(v_str).append(";");
        } else {
          keyBuilder.append("[").append(v_str).append("]");
        }
      }
      context.write(new Text(keyBuilder.toString()), new Text(valueBuilder.toString()));
    }
  }

}
