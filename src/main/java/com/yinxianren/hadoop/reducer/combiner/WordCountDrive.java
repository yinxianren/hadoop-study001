package com.yinxianren.hadoop.reducer.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class WordCountDrive {

	public static void main(String[] args) throws Exception {
		
		
		args=new String[] {"D:\\app\\input\\test001.txt","D:\\app\\output\\test008"};
		
		Configuration conf=new Configuration();  
		//1 ��ȡJob����
		Job  job=Job.getInstance(conf);
		//2 ����jar�洢��λ��
		job.setJarByClass(WordCountDrive.class);
		//3 ���ù���Map��Reduce��
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		//4 ����mapper�׶�������ݵ�key��value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//5 �����������������key�� value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// ָ����Ҫʹ��combiner���Լ����ĸ�����Ϊcombiner���߼�
		job.setCombinerClass(WordcountCombiner.class);
		//6 ��������·�������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 �ύ
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);


	}

}
