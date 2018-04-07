package com.kookmin.three;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 맵의 output 인자와 리듀스의 input 인자는 같아야 한다.
 */
public class RatioReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
    private FloatWritable result = new FloatWritable(0);
    
    private float maxCount;
    private String asin;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
    	maxCount = context.getConfiguration().getFloat("maxCount",0);
    	asin = context.getConfiguration().get("asin");
    }
    
    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values,
            Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        float temp = 0;
        for (FloatWritable val : values) {
        	temp = val.get();
        	if(temp > maxCount) {
            	maxCount = temp;
            	asin = key.toString();
            }
        }
        //context.write(key, result);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	 context.write(new Text(asin),new FloatWritable(maxCount));
    }

}