package com.yinxianren.hadoop.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author root
 *KEYIN: LongWritable  输入数据的 KEY
 *VALUEIN:Text  输入的数据value  
 *
 *KEYOUT: Text  输出数据的key类型  
 *VALUEOUT:IntWritable  输出数据的value类型 
 *
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	private Text k=new Text();
	private IntWritable v=new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, 
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//1 获取一行数据
		String line=value.toString();
		//2 按空格切分
		String[] words=line.split(" ");
		//3 循环写出去
		for(String word:words) {
			k.set(word);
			context.write(k, v);
		}
		
	}
}
