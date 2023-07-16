package org.LPA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import sun.awt.X11.XSystemTrayPeer;

import java.io.IOException;


public class LPAMain {

  public static Job initInitJob(String input_path, String output_path) throws IOException {
    Job job = Job.getInstance(new Configuration(), "LPA Init");
    job.setJarByClass(LPAInit.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(LPAInit.LPAInitMapper.class);
    job.setReducerClass(LPAInit.LPAInitReducer.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(input_path));
    FileOutputFormat.setOutputPath(job, new Path(output_path));
    return job;
  }

  public static void runInit(String input_path, String output_path) {
    try {
      Job init_job = initInitJob(input_path, output_path);
      init_job.waitForCompletion(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static Job initPropagateJob(String input_path, String output_path) throws IOException {
    Job job = Job.getInstance(new Configuration(), "LPA Propagate");
    job.setJarByClass(LPAPropagate.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(LPAPropagate.LPAPropagateMapper.class);
    job.setReducerClass(LPAPropagate.LPAPropagateReducer.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LPAPropagate.LPAPropagateValue.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(input_path));
    FileOutputFormat.setOutputPath(job, new Path(output_path));
    return job;
  }

  public static Job initRebuildJob(String input_path, String output_path) throws IOException {
    Job job = Job.getInstance(new Configuration(), "LPA Rebuild");
    job.setJarByClass(LPARebuild.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapperClass(LPARebuild.LPARebuildMapper.class);
    job.setReducerClass(LPARebuild.LPARebuildReducer.class);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, new Path(input_path));
    FileOutputFormat.setOutputPath(job, new Path(output_path));
    return job;
  }

  public static String runPropagate(String input_path, String output_path) {
    String propagate_path = output_path + "propagate/";
    String rebuild_path = output_path + "rebuild/";
    try {
      Job propagate_job = initPropagateJob(input_path, propagate_path);
      Job rebuild_job = initRebuildJob(propagate_path, rebuild_path);
      ControlledJob propagate_job_ctrl = new ControlledJob(new Configuration());
      propagate_job_ctrl.setJob(propagate_job);
      ControlledJob rebuild_job_ctrl = new ControlledJob(new Configuration());
      rebuild_job_ctrl.setJob(rebuild_job);
      JobControl job_ctrller = new JobControl("Propagete Job Controller");
      job_ctrller.addJob(propagate_job_ctrl);
      job_ctrller.addJob(rebuild_job_ctrl);
      rebuild_job_ctrl.addDependingJob(propagate_job_ctrl);
      Thread job_ctrller_thread = new Thread(job_ctrller);
      job_ctrller_thread.start();
      int time_cnt = 0;
      while (true){

        if(!job_ctrller.allFinished()){
//          System.out.println("running " + String.valueOf(time_cnt));
//          Thread.sleep(5000);
        }else{
          job_ctrller.stop();
          break;
        }
        time_cnt ++;
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
    return rebuild_path;
  }

  public static void main(String[] args) throws IOException {
    String input_path = args[0];
    String output_path = args[1].endsWith("/") ? args[1] : args[1] + "/";
    Integer iterate_time = Integer.parseInt(args[2]);
    String init_output_path = output_path + "init/";
    runInit(input_path, init_output_path);
    String propagate_input_path = init_output_path;
    for (int i = 0; i < iterate_time; i++) {
      String propagate_output_path = output_path + "temp/r" + String.valueOf(i) + "/";
      propagate_input_path = runPropagate(propagate_input_path, propagate_output_path);
    }
  }
}

