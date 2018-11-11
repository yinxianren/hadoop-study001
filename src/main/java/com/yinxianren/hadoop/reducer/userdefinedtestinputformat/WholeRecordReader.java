package com.yinxianren.hadoop.reducer.userdefinedtestinputformat;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends RecordReader<Text, BytesWritable>{

	private Configuration configuration;
	private FileSplit split;

	private boolean isProgress= true;
	private BytesWritable value = new BytesWritable();
	private Text k = new Text();

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

		this.split = (FileSplit)split;
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (isProgress) {

			// 1 ���建����
			byte[] contents = new byte[(int)split.getLength()];

			FileSystem fs = null;
			FSDataInputStream fis = null;

			try {
				// 2 ��ȡ�ļ�ϵͳ
				Path path = split.getPath();
				fs = path.getFileSystem(configuration);

				// 3 ��ȡ����
				fis = fs.open(path);

				// 4 ��ȡ�ļ�����
				IOUtils.readFully(fis, contents, 0, contents.length);

				// 5 ����ļ�����
				value.set(contents, 0, contents.length);

				// 6 ��ȡ�ļ�·��������
				String name = split.getPath().toString();

				// 7 ���������keyֵ
				k.set(name);

			} catch (Exception e) {

			}finally {
				IOUtils.closeStream(fis);
			}

			isProgress = false;

			return true;
		}

		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
	}
}