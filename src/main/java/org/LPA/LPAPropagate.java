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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class LPAPropagate {
  public static class LPAPropagateValue implements WritableComparable<LPAPropagate.LPAPropagateValue> {
    public Text name;
    public Text label;
    public DoubleWritable weight;

    public LPAPropagateValue() {
      this.name = new Text();
      this.label = new Text();
      this.weight = new DoubleWritable();
    }

    public LPAPropagateValue(String name, String label, Double weight) {
      this.name = new Text(name);
      this.label = new Text(label);
      this.weight = new DoubleWritable(weight);
    }

    @Override
    public int compareTo(LPAPropagateValue lpaPropagateValue) {
      int compare_name = this.name.compareTo(lpaPropagateValue.name);
      if (compare_name != 0)
        return compare_name;
      int compare_label = this.name.compareTo(lpaPropagateValue.label);
      if (compare_label == 0)
        return compare_label;
      return this.weight.compareTo(lpaPropagateValue.weight);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.name.write(out);
      this.label.write(out);
      this.weight.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.name.readFields(in);
      this.label.readFields(in);
      this.weight.readFields(in);
    }
  }

  public static class LPAPropagateMapper extends Mapper<Text, Text, Text, LPAPropagateValue> {
    @Override
    protected void map(Text key, Text value, Mapper<Text, Text, Text, LPAPropagateValue>.Context context) throws IOException, InterruptedException {
      String[] nameAndLabel = key.toString().split("[\\[\\]]");
      String key_name = nameAndLabel[0];
      String key_label = nameAndLabel[1];
      String[] outNodes = value.toString().split(";");
      for (String outNode : outNodes) {
        String[] nodeAndWeight = outNode.split(",");
        String nodeName = nodeAndWeight[0];
        Double nodeWeight = Double.parseDouble(nodeAndWeight[1]);
        LPAPropagateValue v = new LPAPropagateValue(key_name, key_label, nodeWeight);
        context.write(new Text(nodeName), v);
      }
    }
  }

  public static class LPAPropagateReducer extends Reducer<Text, LPAPropagateValue, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<LPAPropagateValue> values, Reducer<Text, LPAPropagateValue, Text, Text>.Context context) throws IOException, InterruptedException {
      List<String> inNames = new ArrayList<>();
      List<Double> inWeights = new ArrayList<>();
      Map<String, Double> label_map = new HashMap<>();
      for (LPAPropagateValue v : values) {
        String name = v.name.toString();
        String label = v.label.toString();
        Double weight = v.weight.get();
        inNames.add(name);
        inWeights.add(weight);
        if (label_map.containsKey(label)) {
          label_map.put(label, label_map.get(label) + weight);
        } else {
          label_map.put(label, weight);
        }
      }
      String max_label = Collections.max(label_map.entrySet(), Map.Entry.comparingByValue()).getKey();
      StringBuilder valueBuilder = new StringBuilder();
      for (int i = 0; i < inNames.size(); i++) {
        valueBuilder.append(inNames.get(i)).append(",").append(inWeights.get(i)).append(";");
      }
//      System.out.printf("%s new label :%s\n", key.toString(), max_label);
      context.write(new Text(key.toString() + "[" + max_label + "]"), new Text(valueBuilder.toString()));
    }
  }

  public static void run(String[] args) {
    try {
      Job job = Job.getInstance(new Configuration(), "LPA Propagate");
      job.setJarByClass(LPAInit.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setMapperClass(LPAPropagateMapper.class);
      job.setReducerClass(LPAPropagateReducer.class);
      job.setInputFormatClass(KeyValueTextInputFormat.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(LPAPropagateValue.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    run(args);
  }
}
