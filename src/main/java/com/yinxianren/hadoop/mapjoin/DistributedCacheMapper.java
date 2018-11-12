package com.yinxianren.hadoop.mapjoin;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	Map<String, String> pdMap = new HashMap<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {

		// 1 ��ȡ������ļ�
		URI[] cacheFiles = context.getCacheFiles();
		String path = cacheFiles[0].getPath().toString();
		
		BufferedReader reader = 
				new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
		
		String line;
		while(StringUtils.isNotEmpty(line = reader.readLine())){

			// 2 �и�
			String[] fields = line.split("\t");
			
			// 3 �������ݵ�����
			pdMap.put(fields[0], fields[1]);
		}
		
		// 4 ����
		reader.close();
	}
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 ��ȡһ��
		String line = value.toString();
		
		// 2 ��ȡ
		String[] fields = line.split("\t");
		
		// 3 ��ȡ��Ʒid
		String pId = fields[1];
		
		// 4 ��ȡ��Ʒ����
		String pdName = pdMap.get(pId);
		
		// 5 ƴ��
		k.set(line + "\t"+ pdName);
		
		// 6 д��
		context.write(k, NullWritable.get());
	}
}