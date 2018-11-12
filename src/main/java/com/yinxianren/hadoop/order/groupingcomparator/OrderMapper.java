package com.yinxianren.hadoop.order.groupingcomparator;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author root
0000001	Pdt_01	222.8
0000002	Pdt_05	722.4
0000001	Pdt_02	33.8
0000003	Pdt_06	232.8
0000003	Pdt_02	33.8
0000002	Pdt_03	522.8
0000002	Pdt_04	122.4
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

	OrderBean k = new OrderBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
  //0000001	  Pdt_01	222.8
		// 1 ��ȡһ��
		String line = value.toString();

		// 2 ��ȡ
		String[] fields = line.split("\t");

		// 3 ��װ����
		k.setOrder_id(Integer.parseInt(fields[0]));
		k.setPrice(Double.parseDouble(fields[2]));

		// 4 д��
		context.write(k, NullWritable.get());
	}
}