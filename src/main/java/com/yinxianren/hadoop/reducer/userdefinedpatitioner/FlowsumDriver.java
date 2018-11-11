package com.yinxianren.hadoop.reducer.userdefinedpatitioner;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowsumDriver {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {

		// �������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		args=new String[] {"D:\\app\\input\\test002.txt","D:\\app\\output\\test006"};

		// 1 ��ȡ������Ϣ������job����ʵ��
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ָ���������jar�����ڵı���·��
		job.setJarByClass(FlowsumDriver.class);

		// 3 ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		// 4 ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		// 8 ָ���Զ������ݷ���
		job.setPartitionerClass(ProvincePartitioner.class);

		// 9 ͬʱָ����Ӧ������reduce task
		job.setNumReduceTasks(5);
		
		// 6 ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 ��job�����õ���ز������Լ�job���õ�java�����ڵ�jar���� �ύ��yarnȥ����
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}