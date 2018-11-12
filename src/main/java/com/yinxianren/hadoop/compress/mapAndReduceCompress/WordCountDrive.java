package com.yinxianren.hadoop.compress.mapAndReduceCompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDrive {

	public static void main(String[] args) throws Exception {


		args=new String[] {"D:\\app\\input\\test001.txt","D:\\app\\output\\test001"};

		Configuration conf=new Configuration();  


		// ����map�����ѹ��
		conf.setBoolean("mapreduce.map.output.compress", true);
		// ����map�����ѹ����ʽ
		conf.setClass("mapreduce.map.output.compress.codec", 
				BZip2Codec.class, CompressionCodec.class);

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
		/*  �Ż�hadoop����Ч�ʣ�������Ч���� combiner�����µİ���Ч����һ���� ��  */
		//��һ�����Ǳ���ģ����Ż�����ȥ�ģ�
		job.setCombinerClass(WordCountReduce.class);

		//6 ��������·�������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		//����reduceѹ��
		// ����reduce�����ѹ������
		FileOutputFormat.setCompressOutput(job, true);
		// ����ѹ���ķ�ʽ
		FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); 

		// 7 �ύ
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);


	}

}
