package com.yinxianren.hadoop.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LogDriver {

	public static void main(String[] args) throws Exception {

		// �������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		args = new String[] { "e:/input/inputlog", "e:/output1" };

		// 1 ��ȡjob��Ϣ
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ����jar��
		job.setJarByClass(LogDriver.class);

		// 3 ����map
		job.setMapperClass(LogMapper.class);

		// 4 ���������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// ����reducetask����Ϊ0
		job.setNumReduceTasks(0);

		// 5 ������������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6 �ύ
		job.waitForCompletion(true);
	}
}
