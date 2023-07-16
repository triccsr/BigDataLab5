package org.PageRank;

import org.GraphFileGen.PersonWritableComparable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankMain {
    public static class PRSortMapper extends Mapper<Object, Text, DoubleWritable,Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] personInfo=value.toString().split("\\s*[,;]+",2);
            String[] words=personInfo[1].split("[,;]+");
            double pr=Double.parseDouble(words[0]);
            context.write(new DoubleWritable(-pr),new Text(personInfo[0]));
        }
    }
    public static class PRSortReducer extends Reducer<DoubleWritable,Text,Text,Text>{
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            for(Text t:values){
                context.write(new Text(t), new Text(Double.toString(-key.get())));
            }
        }
    }
    public static void main(String[] args){
        for(String w:args){
            if(w.endsWith("/")){
                w=w.substring(0,w.length()-1);
            }
        }
        PageRankInit.run(new String[]{args[0],args[1]+"/tmp/r0"});
        int times= Integer.parseInt(args[2]);
        for(int i=1;i<=times;++i){
            int last=i-1;
            PageRankWork.run(new String[]{args[1]+"/tmp/r"+last,args[1]+"/tmp/r"+i});
        }

        try {
            Configuration conf=new Configuration();
//            conf.set("fs.defaultFS", "hdfs://localhost:9000");
//            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Job job = Job.getInstance(new Configuration(), "PageRankMain");
            job.setJarByClass(PageRankMain.class);


            //job.setCombinerClass(KNNCombiner.class);
            job.setReducerClass(PageRankMain.PRSortReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(PageRankMain.PRSortMapper.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[1]+"/tmp/r"+times));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/result"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
