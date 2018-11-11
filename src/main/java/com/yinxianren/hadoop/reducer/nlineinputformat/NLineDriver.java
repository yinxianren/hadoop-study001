package com.yinxianren.hadoop.reducer.nlineinputformat;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineDriver {

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {

		// �������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		args=new String[] {"D:\\app\\input\\test004.txt","D:\\app\\output\\test004"};

		// 1 ��ȡjob����
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 7����ÿ����ƬInputSplit�л���������¼
		NLineInputFormat.setNumLinesPerSplit(job, 3);

		// 8ʹ��NLineInputFormat�����¼��  
		job.setInputFormatClass(NLineInputFormat.class);  

		// 2����jar��λ�ã�����mapper��reducer
		job.setJarByClass(NLineDriver.class);  
		job.setMapperClass(NLineMapper.class);  
		job.setReducerClass(NLineReducer.class);  

		// 3����map���kv����
		job.setMapOutputKeyClass(Text.class);  
		job.setMapOutputValueClass(LongWritable.class);  

		// 4�����������kv����
		job.setOutputKeyClass(Text.class);  
		job.setOutputValueClass(LongWritable.class);  

		// 5���������������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));  
		FileOutputFormat.setOutputPath(job, new Path(args[1]));  

		// 6�ύjob
		job.waitForCompletion(true);  
	}
}