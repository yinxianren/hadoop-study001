package com.yinxianren.hadoop.example.invertedOrder;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 
 * @author root
 * 
 *数据源
 atguigu--a.txt	3
atguigu--b.txt	2
atguigu--c.txt	2
c.txt--c.txt	1
pingping--a.txt	1
pingping--b.txt	3
pingping--c.txt	1
ss--a.txt	2
ss--b.txt	1
ss--c.txt	1


结果
atguigu	c.txt-->2	b.txt-->2	a.txt-->3	
pingping	c.txt-->1	b.txt-->3	a.txt-->1	
ss	c.txt-->1	b.txt-->1	a.txt-->2	


 */
public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text>{

	Text k = new Text();
	Text v = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// 1 获取1行数据    atguigu--a.txt	3
		String line = value.toString();
		
		// 2用“--”切割
		String[] fields = line.split("--");
		
		k.set(fields[0]);
		v.set(fields[1]);
		
		// 3 输出数据
		context.write(k, v);
	}
}
