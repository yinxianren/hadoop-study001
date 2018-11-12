package com.yinxianren.hadoop.etl;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * �����к���ҵ��MapReduce����֮ǰ������Ҫ�ȶ����ݽ�����ϴ��������������û�Ҫ������ݡ�
 * ����Ĺ�������ֻ��Ҫ����Mapper���򣬲���Ҫ����Reduce����
 * @author root
 *
 * ���������ǣ�log.txt
 *
 */


public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// 1 ��ȡ1������
		String line = value.toString();
		
		// 2 ������־
		boolean result = parseLog(line,context);
		
		// 3 ��־���Ϸ��˳�
		if (!result) {
			return;
		}
		
		// 4 ����key
		k.set(line);
		
		// 5 д������
		context.write(k, NullWritable.get());
	}
    /**
     * ȥ����־���ֶγ���С�ڵ���11����־��
     * @param line
     * @param context
     * @return
     */
	// 2 ������־
	private boolean parseLog(String line, Context context) {

		// 1 ��ȡ
		String[] fields = line.split(" ");
		
		// 2 ��־���ȴ���11��Ϊ�Ϸ�
		if (fields.length > 11) {

			// ϵͳ������
			context.getCounter("map", "true").increment(1);
			return true;
		}else {
			context.getCounter("map", "false").increment(1);
			return false;
		}
	}
}
