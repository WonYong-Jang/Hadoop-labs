package com.kookmin.three;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
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
public class RatioMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private Text word = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context)
            throws IOException, InterruptedException {
    	String asin, tempHelpful;
    	int a=0, b=0;
    	float resultRatio= 0;
        String line = value.toString();
        String[] tuple = line.split("\\n"); // 라인별로 가져옴
        try {
        	for(int i=0; i<tuple.length; i++) {
        		JSONObject obj = new JSONObject(tuple[i]); // json으로 파싱
        		tempHelpful = obj.getString("helpful");  //helpful value 가져오기 [a, b]
        		a= tempHelpful.charAt(1) - '0'; //  char -> int
        		b = tempHelpful.charAt(4) - '0';
        		if(b > 10) // b가 10보다 크다면
        		{
        			resultRatio = (float)a/(float)b; // 비율 구해서 context에 write
        			asin = obj.getString("asin"); // asin value 값 
        			word.set(asin);
        			context.write(word , new FloatWritable(resultRatio));
        		}
        	}
        }catch(JSONException e) {
        	e.printStackTrace();
        }
        	
    }
}