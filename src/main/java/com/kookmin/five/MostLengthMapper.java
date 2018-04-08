package com.kookmin.five;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * reviewText 길이가 가장 긴 reviewerName 을 찾는 문제
 */
public class MostLengthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
    	String reviewerName, reviewText;
    	int len;
        String line = value.toString();
        String[] tuple = line.split("\\n"); // 라인별로 
        try {
        	for(int i=0; i<tuple.length; i++) {
        		JSONObject obj = new JSONObject(tuple[i]); // json으로 파싱
        		reviewText = obj.getString("reviewText");
        		len = reviewText.length(); // 리뷰텍스트 길이 구하기
        		reviewerName = obj.getString("reviewerName"); //key값 overall을 이용해서 value 값을 찾는다.
        		context.write(new Text(reviewerName), new IntWritable(len));
        	}
        }catch(JSONException e) {
        	e.printStackTrace();
        }
    }
}