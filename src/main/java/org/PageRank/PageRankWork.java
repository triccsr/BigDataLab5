package org.PageRank;

import org.GraphFileGen.PersonWritableComparable;
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
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;

public class PageRankWork {
    public static final double D=0.85;
    public static class PRWorkMapper extends Mapper<Object, Text, PersonWritableComparable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] personInfo=value.toString().split("\\s*,+",2);
            String[] words=personInfo[1].split(",+");
            double pr=Double.parseDouble(words[0]);
            //words[1]=N
            for(int i=2;i<words.length;i+=2){
                double proportion=Double.parseDouble(words[i+1]);
                context.write(new PersonWritableComparable(words[i]),new Text("+"+proportion * pr));
            }
            context.write(new PersonWritableComparable(personInfo[0]),new Text(personInfo[1]));
        }
    }
    public static class PRWorkReducer extends Reducer<PersonWritableComparable,Text,Text,Text>{
        @Override
        public void reduce(PersonWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String info="";
            double ans=0.0;
            for(Text t:values){
                String s=t.toString();
                if(s.startsWith("+")){//caculate
                    String[] db=s.split("\\+",1);
                    ans+=Double.parseDouble(db[0]);
                }
                else{//info
                    info=t.toString();
                }
            }
            String[] words=info.split(",+");
            int N=Integer.parseInt(words[1]);
            ans=(1.0-D)/(double)N+D*ans;
            StringBuilder sb=new StringBuilder(",,,"+Double.toString(ans)+",,"+N+",,");
            for(int i=2;i< words.length;++i){
                sb.append(words[i]);
                sb.append(',');
            }
            context.write(new Text(key.toString()),new Text(sb.toString()));
        }
    }

    public static int run(String[] args) {
        try {
            Configuration conf=new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            Job job = Job.getInstance(new Configuration(), "PageRankWork");
            job.setJarByClass(PageRankWork.class);


            //job.setCombinerClass(KNNCombiner.class);
            job.setReducerClass(PageRankWork.PRWorkReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(PageRankWork.PRWorkMapper.class);
            job.setMapOutputKeyClass(PersonWritableComparable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            return (job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }
    public static void main(String[] args){
        run(args);
    }
}
