package com.yinxianren.hadoop.reducer.nlineinputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	LongWritable v = new LongWritable();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,	Context context) throws IOException, InterruptedException {
		
        long sum = 0l;

        // 1 »ã×Ü
        for (LongWritable value : values) {
            sum += value.get();
        }  
        
        v.set(sum);
        
        // 2 Êä³ö
        context.write(key, v);
	}
}