package com.yinxianren.hadoop.order.distingsorts;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountSortDriver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// �������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		args=new String[] {"D:\\app\\input\\test005.txt","D:\\app\\output\\test008"};

		// 1 ��ȡ������Ϣ������job����ʵ��
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ָ���������jar�����ڵı���·��
		job.setJarByClass(FlowCountSortDriver.class);

		// 3 ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);

		// 4 ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);

		// 5 ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// �����Զ��������
		job.setPartitionerClass(ProvincePartitioner.class);
		// ����Reducetask����
		job.setNumReduceTasks(5);
		
		// 6 ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 ��job�����õ���ز������Լ�job���õ�java�����ڵ�jar���� �ύ��yarnȥ����
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}