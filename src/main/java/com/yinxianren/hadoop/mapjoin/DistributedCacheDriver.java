package com.yinxianren.hadoop.mapjoin;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedCacheDriver {

	public static void main(String[] args) throws Exception {

		// 0 �����Լ�����·����������
		args = new String[]{"e:/input/inputtable2", "e:/output1"};

		// 1 ��ȡjob��Ϣ
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 ���ü���jar��·��
		job.setJarByClass(DistributedCacheDriver.class);

		// 3 ����map
		job.setMapperClass(DistributedCacheMapper.class);

		// 4 �������������������
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 5 �����������·��
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 6 ���ػ�������
		job.addCacheFile(new URI("file:///e:/input/inputcache/pd.txt"));

		// 7 Map��Join���߼�����ҪReduce�׶Σ�����reduceTask����Ϊ0
		job.setNumReduceTasks(0);

		// 8 �ύ
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}