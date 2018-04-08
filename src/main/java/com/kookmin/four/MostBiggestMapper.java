package com.kookmin.four;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 *  맵을 만들기 위해서 매퍼를 상속
 *	매퍼 클래스는 4개 인자를 받음 => 하둡 전용 자료형 ( String 은 text로 사용함)
 *	리듀스로 key, value 쌍을 보내는 작업을 context.write
 */
public class MostBiggestMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
    	String reviewerID, helpful;
        String line = value.toString();
        String[] tuple = line.split("\\n"); // 라인별로 
        try {
        	for(int i=0; i<tuple.length; i++) {
        		JSONObject obj = new JSONObject(tuple[i]); // json으로 파싱
        		helpful = obj.getString("helpful"); // helpful value
        		reviewerID = obj.getString("reviewerID"); //id값
        		context.write(new Text(reviewerID), new Text(helpful));
        	}
        }catch(JSONException e) {
        	e.printStackTrace();
        }
    }
}