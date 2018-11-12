package com.yinxianren.hadoop.userdefinedoutputformat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	Text k = new Text();

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values, Context context)		throws IOException, InterruptedException {

		// 1 ��ȡһ��
		String line = key.toString();

		// 2 ƴ��
		line = line + "\r\n";

		// 3 ����key
		k.set(line);

		// 4 ���
		context.write(k, NullWritable.get());
	}
}
