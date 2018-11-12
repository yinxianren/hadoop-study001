package com.yinxianren.hadoop.reducer.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
  
	private IntWritable v=new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> valus,
			Context context) throws IOException, InterruptedException {
		
		int sum=0;
		//1 累加求和
		for(IntWritable value:valus) {
			sum+=value.get();
		}
		
		v.set(sum);
		//2 写出数据
		context.write(key,v);
		
		
	}
	
	
	
	
}
