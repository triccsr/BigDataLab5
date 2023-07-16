package org.PageRank;

import org.GraphFileGen.GraphFileGen;
import org.GraphFileGen.PersonWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class PageRankInit {
    public static class PRInitMapper extends Mapper<Object,Text, IntWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            context.write(new IntWritable(1),value);
        }
    }
    public static class PRInitReducer extends Reducer<IntWritable,Text,Text,Text>{
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int personCount=0;
            ArrayList<String> arrayList=new ArrayList<>();
            for(Text t:values){
                personCount+=1;
                arrayList.add(t.toString());
            }
            for(String s:arrayList){
                String[] personEdges=s.split("\\s\\[",2);
                String[] words=personEdges[1].split("[\\|\\],]+");
                StringBuilder sb=new StringBuilder(";"+Double.toString(1.0/(double)personCount));
                sb.append(";");
                sb.append(personCount);
                sb.append(";");
                for(int i=0;i<words.length;++i){
                    sb.append(words[i]);
                    sb.append(",");
                }
                context.write(new Text(personEdges[0]),new Text(sb.toString()));
            }
        }
    }

    public static int run(String[] args) {
        try {
            Configuration conf=new Configuration();
//            conf.set("fs.defaultFS", "hdfs://localhost:9000");
//            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Job job = Job.getInstance(new Configuration(), "PageRankInit");
            job.setJarByClass(PageRankInit.class);


            //job.setCombinerClass(KNNCombiner.class);
            job.setReducerClass(PageRankInit.PRInitReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(PageRankInit.PRInitMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            return (job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
    public static int main(String[] args){
        run(args);
        return 0;
    }
}
