package com.yinxianren.hadoop.reducer.keyvaluetextinputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVTextDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		args=new String[] {"D:\\app\\input\\test003.txt","D:\\app\\output\\test003"};
		
		Configuration conf = new Configuration();
		// �����и��
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		// 1 ��ȡjob����
		Job job = Job.getInstance(conf);

		// 2 ����jar��λ�ã�����mapper��reducer
		job.setJarByClass(KVTextDriver.class);
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);

		// 3 ����map���kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// 4 �����������kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// 5 ���������������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		// ���������ʽ
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 6 �����������·��
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 �ύjob
		job.waitForCompletion(true);
	}
}
