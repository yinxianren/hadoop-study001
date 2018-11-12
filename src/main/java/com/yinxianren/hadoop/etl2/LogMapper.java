package com.yinxianren.hadoop.etl2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * ����
 * ��Web������־�еĸ��ֶ�ʶ���з֣�ȥ����־�в��Ϸ��ļ�¼��������ϴ����������˺������
 * 
 * �����������
 * ���ǺϷ�������
 * 
 * @author root
 * �������ݣ�log.txt
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	Text k = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {

		// 1 ��ȡ1��
		String line = value.toString();

		// 2 ������־�Ƿ�Ϸ�
		LogBean bean = parseLog(line);

		if (!bean.isValid()) {
			return;
		}

		k.set(bean.toString());

		// 3 ���
		context.write(k, NullWritable.get());
	}

	// ������־
	private LogBean parseLog(String line) {

		LogBean logBean = new LogBean();

		// 1 ��ȡ
		String[] fields = line.split(" ");

		if (fields.length > 11) {

			// 2��װ����
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

			// ����400��HTTP����
			if (Integer.parseInt(logBean.getStatus()) >= 400) {
				logBean.setValid(false);
			}
		}else {
			logBean.setValid(false);
		}

		return logBean;
	}
}
