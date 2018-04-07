package com.kookmin.one;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
/**
 * JobConf를 통해서 Hadoop의 MapReduce를 이용할 것이며, JobConf가 알아서 map function과 reducer funtion call
 * JobConf를 통해서 Mapper와 Reducer의 input 그리고 output Type을 설정
 */
public class Overall {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.out.println("Usage: average <input> <output>");
            System.exit(2);
        }
        Job job = new Job(conf, "Overall");
        job.setJarByClass(Overall.class);
        job.setMapperClass(OverallMapper.class);
        job.setReducerClass(OverallReducer.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
         
        job.setOutputKeyClass(IntWritable.class); // input으로 받아서 mapper에서 output type
        job.setOutputValueClass(DoubleWritable.class);
         
        FileInputFormat.addInputPath(job, new Path(args[0])); //input 경로
        FileOutputFormat.setOutputPath(job, new Path(args[1])); //ouput 경로
        job.waitForCompletion(true); // 실행
    }
}
