package com.yinxianren.hadoop.userdefinedoutputformat;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

	FSDataOutputStream atguiguOut = null;
	FSDataOutputStream otherOut = null;

	public FilterRecordWriter(TaskAttemptContext job) {

		// 1 ��ȡ�ļ�ϵͳ
		FileSystem fs;

		try {
			fs = FileSystem.get(job.getConfiguration());

			// 2 ��������ļ�·��
			Path atguiguPath = new Path("D:/app/output/test010/atguigu.log");
			Path otherPath = new Path("D:/app/output/test010/other.log");

			// 3 ���������
			atguiguOut = fs.create(atguiguPath);
			otherOut = fs.create(otherPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {

		// �ж��Ƿ������atguigu���������ͬ�ļ�
		if (key.toString().contains("atguigu")) {
			atguiguOut.write(key.toString().getBytes());
		} else {
			otherOut.write(key.toString().getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {

		// �ر���Դ
		IOUtils.closeStream(atguiguOut);	
		IOUtils.closeStream(otherOut);	}
}