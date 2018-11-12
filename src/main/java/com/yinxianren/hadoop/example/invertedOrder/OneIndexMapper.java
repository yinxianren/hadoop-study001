package com.yinxianren.hadoop.example.invertedOrder;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/*
 *
 * a.txt
atguigu pingping
atguigu ss
atguigu ss
 * 
 * b.txt
atguigu pingping
atguigu pingping
pingping ss

c.txt
atguigu ss
atguigu pingping

���
atguigu--a.txt	3
atguigu--b.txt	2
atguigu--c.txt	2
pingping--a.txt	1
pingping--b.txt	3
pingping--c.txt	1
ss--a.txt	2
ss--b.txt	1
ss--c.txt	1

 */
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	String name;
	Text k = new Text();
	IntWritable v = new IntWritable();
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {

		// ��ȡ�ļ�����
		FileSplit split = (FileSplit) context.getInputSplit();
		
		name = split.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

		// 1 ��ȡ1��
		String line = value.toString();
		
		// 2 �и�
		String[] fields = line.split(" ");
		
		for (String word : fields) {

			// 3 ƴ��
			k.set(word+"--"+name);
			v.set(1);
			
			// 4 д��
			context.write(k, v);
		}
	}
}