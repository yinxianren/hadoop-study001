package com.yinxianren.hadoop.reducejoin;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
/**
 * 
 * @author root
 *
 *order.txt
1001	01	1
1002	02	2
1003	03	3
1004	01	4
1005	02	5
1006	03	6
 *
 *pd.txt
01	С��
02	��Ϊ
03	����
 *
 *
 *���
1001	С��	1	
1001	С��	1	
1002	��Ϊ	2	
1002	��Ϊ	2	
1003	����	3	
1003	����	3	
 *
 *
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{

	String name;
	TableBean bean = new TableBean();
	Text k = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// 1 ��ȡ�����ļ���Ƭ
		FileSplit split = (FileSplit) context.getInputSplit();

		// 2 ��ȡ�����ļ�����
		name = split.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 ��ȡ��������
		String line = value.toString();

		// 2 ��ͬ�ļ��ֱ���
		if (name.startsWith("order")) {// ��������

			// 2.1 �и�
			String[] fields = line.split("\t");

			// 2.2 ��װbean����
			bean.setOrder_id(fields[0]);
			bean.setP_id(fields[1]);
			bean.setAmount(Integer.parseInt(fields[2]));
			bean.setPname("");
			bean.setFlag("order");

			k.set(fields[1]);
		}else {// ��Ʒ����

			// 2.3 �и�
			String[] fields = line.split("\t");

			// 2.4 ��װbean����
			bean.setP_id(fields[0]);
			bean.setPname(fields[1]);
			bean.setFlag("pd");
			bean.setAmount(0);
			bean.setOrder_id("");

			k.set(fields[0]);
		}

		// 3 д��
		context.write(k, bean);
	}
}
