package com.yinxianren.hadoop.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class WordCountDrive {

	public static void main(String[] args) throws Exception {
		
		
		args=new String[] {"D:\\app\\input\\test001.txt","D:\\app\\output\\test001"};
		
		Configuration conf=new Configuration();  
		//1 获取Job对象
		Job  job=Job.getInstance(conf);
		//2 设置jar存储的位置
		job.setJarByClass(WordCountDrive.class);
		//3 设置关联Map和Reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);
		//4 设置mapper阶段输出数据的key和value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		//5 设置最终数据输出的key和 value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
        /*  优化hadoop运行效率，产生的效果和 combiner包底下的案例效果是一样的 ；  */
		//这一步不是必须的，是优化加上去的；
		job.setCombinerClass(WordCountReduce.class);
		
		//6 设置输入路径和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);


	}

}
