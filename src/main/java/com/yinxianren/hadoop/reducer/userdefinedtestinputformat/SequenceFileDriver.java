package com.yinxianren.hadoop.reducer.userdefinedtestinputformat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceFileDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// �������·����Ҫ�����Լ�������ʵ�ʵ��������·������
		args = new String[] { "D:\\app\\input", "D:\\app\\output\\test005" };

		// 1 ��ȡjob����
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2 ����jar���洢λ�á������Զ����mapper��reducer
		job.setJarByClass(SequenceFileDriver.class);
		job.setMapperClass(SequenceFileMapper.class);
		job.setReducerClass(SequenceFileReducer.class);

		// 7���������inputFormat
		job.setInputFormatClass(WholeFileInputformat.class);

		// 8���������outputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 3 ����map����˵�kv����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		// 4 ������������˵�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// 5 �����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6 �ύjob
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
