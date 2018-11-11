package com.yinxianren.hadoop.reducer.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDrive {
	/**
	 * 
	 * @param args
	 * 
	 * 1 ��ȡ������Ϣ������job����ʵ��
	 * 2 ����jar��·��
	 * 3 ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
	 * 4 ָ��mapper������ݵ�key ��value
	 * 5 �����������key ��value
	 * 6 �����������·��
	 * 7 �ύjob
	 * 
	 */
	public static void main(String[] args) throws Exception{

		args=new String[] {"D:\\app\\input\\test002.txt","D:\\app\\output\\test002"};
		Configuration conf=new Configuration();
		//1  ����job
		Job job=Job.getInstance(conf);
		//2  ����jar ·��
		job.setJarByClass(FlowCountDrive.class);


		// 3 ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		// 4 ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		//����������Ҫ�ǽ��С�ļ��ر��������ʹ�ã�ƽ���ļ��Ƚϴ󣬿��� �������ã�
		// 5.1���������InputFormat����Ĭ���õ���TextInputFormat.class
		job.setInputFormatClass(CombineTextInputFormat.class);
		//5.2����洢��Ƭ���ֵ����32m
		CombineTextInputFormat.setMaxInputSplitSize(job, 1024*1204*32);

		// 6 ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		
		
		// 7 ��job�����õ���ز������Լ�job���õ�java�����ڵ�jar���� �ύ��yarnȥ����
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}



}
