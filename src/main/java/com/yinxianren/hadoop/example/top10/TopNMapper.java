package com.yinxianren.hadoop.example.top10;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author root
输入数据
13470253144	180	180	360
13509468723	7335	110349	117684
13560439638	918	4938	5856
13568436656	3597	25635	29232
13590439668	1116	954	2070
13630577991	6960	690	7650
13682846555	1938	2910	4848
13729199489	240	0	240
13736230513	2481	24681	27162
13768778790	120	120	240
13846544121	264	0	264
13956435636	132	1512	1644
13966251146	240	0	240
13975057813	11058	48243	59301
13992314666	3008	3720	6728
15043685818	3659	3538	7197
15910133277	3156	2936	6092
15959002129	1938	180	2118
18271575951	1527	2106	3633
18390173782	9531	2412	11943
84188413	4116	1432	5548

输出数据
13509468723	7335	110349	117684
13975057813	11058	48243	59301
13568436656	3597	25635	29232
13736230513	2481	24681	27162
18390173782	9531	2412	11943
13630577991	6960	690	7650
15043685818	3659	3538	7197
13992314666	3008	3720	6728
15910133277	3156	2936	6092
13560439638	918	4938	5856


 */
public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	// 定义一个TreeMap作为存储数据的容器（天然按key排序）
	private TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();
	private FlowBean kBean;
	
	@Override
	protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		
		kBean = new FlowBean();
		Text v = new Text();
		
		// 1 获取一行
		String line = value.toString();
		
		// 2 切割
		String[] fields = line.split("\t");
		
		// 3 封装数据
		String phoneNum = fields[0];
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		long sumFlow = Long.parseLong(fields[3]);
		
		kBean.setDownFlow(downFlow);
		kBean.setUpFlow(upFlow);
		kBean.setSumFlow(sumFlow);
		
		v.set(phoneNum);
		
		// 4 向TreeMap中添加数据
		flowMap.put(kBean, v);
		
		// 5 限制TreeMap的数据量，超过10条就删除掉流量最小的一条数据
		if (flowMap.size() > 10) {
//		flowMap.remove(flowMap.firstKey());
			flowMap.remove(flowMap.lastKey());		
}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		// 6 遍历treeMap集合，输出数据
		Iterator<FlowBean> bean = flowMap.keySet().iterator();

		while (bean.hasNext()) {

			FlowBean k = bean.next();

			context.write(k, flowMap.get(k));
		}
	}
}