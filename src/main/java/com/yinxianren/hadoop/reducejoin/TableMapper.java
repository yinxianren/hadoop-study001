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
01	小米
02	华为
03	格力
 *
 *
 *结果
1001	小米	1	
1001	小米	1	
1002	华为	2	
1002	华为	2	
1003	格力	3	
1003	格力	3	
 *
 *
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean>{

	String name;
	TableBean bean = new TableBean();
	Text k = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		// 1 获取输入文件切片
		FileSplit split = (FileSplit) context.getInputSplit();

		// 2 获取输入文件名称
		name = split.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 获取输入数据
		String line = value.toString();

		// 2 不同文件分别处理
		if (name.startsWith("order")) {// 订单表处理

			// 2.1 切割
			String[] fields = line.split("\t");

			// 2.2 封装bean对象
			bean.setOrder_id(fields[0]);
			bean.setP_id(fields[1]);
			bean.setAmount(Integer.parseInt(fields[2]));
			bean.setPname("");
			bean.setFlag("order");

			k.set(fields[1]);
		}else {// 产品表处理

			// 2.3 切割
			String[] fields = line.split("\t");

			// 2.4 封装bean对象
			bean.setP_id(fields[0]);
			bean.setPname(fields[1]);
			bean.setFlag("pd");
			bean.setAmount(0);
			bean.setOrder_id("");

			k.set(fields[0]);
		}

		// 3 写出
		context.write(k, bean);
	}
}
