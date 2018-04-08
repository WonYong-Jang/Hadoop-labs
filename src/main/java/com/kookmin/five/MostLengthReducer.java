package com.kookmin.five;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 맵의 output 인자와 리듀스의 input 인자는 같아야 한다.
 * 
 */
public class MostLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private DoubleWritable result = new DoubleWritable();
    
    private int maxLen;
    private String reviewerName;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
    	maxLen = context.getConfiguration().getInt("maxLen", 0); //global 변수
    	reviewerName = context.getConfiguration().get("reviewerName"); //global 변수
    }
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
    	int cnt = 0;
        for (IntWritable val : values) {
            cnt = val.get(); 
            if(cnt > maxLen) { //가장긴 텍스트 찾기
            	maxLen = cnt;
            	reviewerName = key.toString();
            }
        }
      
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	 context.write(new Text(reviewerName),new IntWritable(maxLen)); //결과 write
    }
}
