package com.kookmin.four;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 맵의 output 인자와 리듀스의 input 인자는 같아야 한다.
 * 
 */
public class MostBiggestReducer extends Reducer<Text, Text, Text, IntWritable>{
    private DoubleWritable result = new DoubleWritable();
    
    private int valueA;
    private String reviewerID;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
    	valueA = context.getConfiguration().getInt("valueA", 0);
    	reviewerID = context.getConfiguration().get("reviewerID");
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
    	int a=0, b=0;
    	String tempHelpful;
        for (Text val : values) {
            tempHelpful = val.toString();
        	a= tempHelpful.charAt(1) - '0'; //  char -> int
    		b = tempHelpful.charAt(4) - '0';
            if(a > valueA) { // 가장 큰 a 찾기
            	valueA = a;
            	reviewerID = key.toString();
            }
        }
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	 context.write(new Text(reviewerID),new IntWritable(valueA));
    }
}