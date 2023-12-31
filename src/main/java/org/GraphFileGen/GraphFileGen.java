package org.GraphFileGen;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;


public class GraphFileGen {

    private static class NeighborWritable implements Writable {
        PersonWritableComparable to;
        long count;

        public NeighborWritable(){
            to=new PersonWritableComparable();
            count=0;
        }

        public NeighborWritable(PersonWritableComparable argTo,long argCount){
            to=argTo;
            count=argCount;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            to.write(dataOutput);
            dataOutput.writeLong(count);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            to.readFields(dataInput);
            count=dataInput.readLong();
        }
    }
    public static class GraphFileGenMapper extends Mapper<Object,Text,PersonWritableComparable,NeighborWritable>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] tokens=value.toString().split("\\s*[<>,]\\s*");
            int start=(tokens[0].isEmpty())?1:0;
            context.write(new PersonWritableComparable(tokens[start+0]),new NeighborWritable(new PersonWritableComparable(tokens[start+1]),Long.parseLong(tokens[start+2])));
        }
    }

    public static class GraphFileGenReducer extends Reducer<PersonWritableComparable,NeighborWritable,Text,Text>{
        @Override
        public void reduce(PersonWritableComparable key,Iterable<NeighborWritable> values, Context context) throws IOException, InterruptedException{
            HashMap<PersonWritableComparable,Long> count=new HashMap<>();
            long sum=0;
            for(NeighborWritable value:values){
                sum+=value.count;
                count.compute(value.to.clone(),(k,v)->(v==null)?value.count:(v+value.count));
            }
            StringBuilder sb=new StringBuilder("[");
            boolean firstString=true;
            for(PersonWritableComparable to:count.keySet()) {
                String str = String.format("%s,%.16f", to.toString(), ((double) count.getOrDefault(to, 0l)) / (double) sum);
                if (!firstString) {
                    sb.append('|');
                }
                firstString = false;
                sb.append(str);
            }
            sb.append(']');
            context.write(new Text(key.toString()),new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();

            Job job = Job.getInstance(new Configuration(), "GraphFileGen");
            job.setJarByClass(GraphFileGen.class);


            job.setReducerClass(GraphFileGenReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(GraphFileGenMapper.class);
            job.setMapOutputKeyClass(PersonWritableComparable.class);
            job.setMapOutputValueClass(NeighborWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}