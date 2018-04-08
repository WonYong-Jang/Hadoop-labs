package com.kookmin.one;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * 맵의 output 인자와 리듀스의 input 인자는 같아야 한다.
 * 
 */
public class OverallReducer extends Reducer<IntWritable, DoubleWritable, NullWritable, DoubleWritable>{
    private DoubleWritable result = new DoubleWritable();
 
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
            Reducer<IntWritable, DoubleWritable, NullWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
        double sum = 0;
        double count=0;
        double average=0;
        for (DoubleWritable val : values) { //키값은 1로 같고 전체 평균을 구해 나간다.
            sum += val.get(); //전체 합 구하기
            count++; // 평균을 구하기 위한 count
        }
        average = sum / count; // 평균구하기
        result.set(average); // 전체 평균 set
        
        context.write(NullWritable.get(), result);
    }
}
