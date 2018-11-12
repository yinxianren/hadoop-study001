package com.yinxianren.hadoop.etl2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 需求
 * 对Web访问日志中的各字段识别切分，去除日志中不合法的记录。根据清洗规则，输出过滤后的数据
 * 
 * 期望输出数据
 * 都是合法的数据
 * 
 * @author root
 * 输入数据：log.txt
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

		// 1 获取1行
		String line = value.toString();

		// 2 解析日志是否合法
		LogBean bean = parseLog(line);

		if (!bean.isValid()) {
			return;
		}

		k.set(bean.toString());

		// 3 输出
		context.write(k, NullWritable.get());
	}

	// 解析日志
	private LogBean parseLog(String line) {

		LogBean logBean = new LogBean();

		// 1 截取
		String[] fields = line.split(" ");

		if (fields.length > 11) {

			// 2封装数据
			logBean.setRemote_addr(fields[0]);
			logBean.setRemote_user(fields[1]);
			logBean.setTime_local(fields[3].substring(1));
			logBean.setRequest(fields[6]);
			logBean.setStatus(fields[8]);
			logBean.setBody_bytes_sent(fields[9]);
			logBean.setHttp_referer(fields[10]);

			if (fields.length > 12) {
				logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
			}else {
				logBean.setHttp_user_agent(fields[11]);
			}

			// 大于400，HTTP错误
			if (Integer.parseInt(logBean.getStatus()) >= 400) {
				logBean.setValid(false);
			}
		}else {
			logBean.setValid(false);
		}

		return logBean;
	}
}
