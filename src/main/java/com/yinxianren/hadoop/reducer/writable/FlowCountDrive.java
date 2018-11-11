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
	 * 1 获取配置信息，或者job对象实例
	 * 2 设置jar的路径
	 * 3 指定本业务job要使用的mapper/Reducer业务类
	 * 4 指定mapper输出数据的key 和value
	 * 5 设置最终输出key 和value
	 * 6 设置输入输出路径
	 * 7 提交job
	 * 
	 */
	public static void main(String[] args) throws Exception{

		args=new String[] {"D:\\app\\input\\test002.txt","D:\\app\\output\\test002"};
		Configuration conf=new Configuration();
		//1  创建job
		Job job=Job.getInstance(conf);
		//2  设置jar 路径
		job.setJarByClass(FlowCountDrive.class);


		// 3 指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);

		// 4 指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		// 5 指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		//以下两点主要是解决小文件特别多的情况下使用，平常文件比较大，可以 不是设置；
		// 5.1如果不设置InputFormat，它默认用的是TextInputFormat.class
		job.setInputFormatClass(CombineTextInputFormat.class);
		//5.2虚拟存储切片最大值设置32m
		CombineTextInputFormat.setMaxInputSplitSize(job, 1024*1204*32);

		// 6 指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		
		
		
		// 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}



}
