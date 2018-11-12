package com.yinxianren.hadoop.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {

	public static void main(String[] args) throws Exception {

		// 0 �����Լ�����·����������
		args=new String[] {"D:\\app\\input\\test011.txt","D:\\app\\output\\test011"};

		// 1 ��ȡ������Ϣ������job����ʵ��
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ָ���������jar�����ڵı���·��
		job.setJarByClass(TableDriver.class);

		// 3 ָ����ҵ��jobҪʹ�õ�Mapper/Reducerҵ����
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		// 4 ָ��Mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);

		// 5 ָ��������������ݵ�kv����
		job.setOutputKeyClass(TableBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 ��job�����õ���ز������Լ�job���õ�java�����ڵ�jar���� �ύ��yarnȥ����
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}