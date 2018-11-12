package com.yinxianren.hadoop.order.groupingcomparator;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {

	public static void main(String[] args) throws Exception, IOException {

		// �������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		args=new String[] {"D:\\app\\input\\test009.txt","D:\\app\\output\\test009"};

		// 1 ��ȡ������Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ����jar������·��
		job.setJarByClass(OrderDriver.class);

		// 3 ����map/reduce��
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		// 4 ����map�������key��value����
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 ��������������ݵ�key��value����
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 �����������ݺ��������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

//		 8 ����reduce�˵ķ���
		job.setGroupingComparatorClass(OrderGroupingComparator.class);

		// 7 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}