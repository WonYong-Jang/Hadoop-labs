package com.kookmin.two;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 맵의 output 인자와 리듀스의 input 인자는 같아야 한다.
 */
public class MostReviewsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    //private IntWritable result = new IntWritable();
    
    private int maxCount;
    private String reviewer;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
    	maxCount = context.getConfiguration().getInt("maxCount", 0);
    	reviewer = context.getConfiguration().get("reviewer"); // global 변수 사용하기 위해
    }
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();  // reviewer 별로 갯수 count
        }
        if(sum > maxCount) { // 갯수가 더 많은 reviewer 가 있다면
        	maxCount = sum;
        	reviewer = key.toString();
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	 context.write(new Text(reviewer),new IntWritable(maxCount)); //Reducer가 끝난 후 
    }

}