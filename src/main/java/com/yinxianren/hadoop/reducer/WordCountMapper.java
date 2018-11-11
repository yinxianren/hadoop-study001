package com.yinxianren.hadoop.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author root
 *KEYIN: LongWritable  �������ݵ� KEY
 *VALUEIN:Text  ���������value  
 *
 *KEYOUT: Text  ������ݵ�key����  
 *VALUEOUT:IntWritable  ������ݵ�value���� 
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
		//1 ��ȡһ������
		String line=value.toString();
		//2 ���ո��з�
		String[] words=line.split(" ");
		//3 ѭ��д��ȥ
		for(String word:words) {
			k.set(word);
			context.write(k, v);
		}
		
	}
}
