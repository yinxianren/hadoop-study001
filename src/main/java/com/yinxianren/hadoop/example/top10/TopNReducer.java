package com.yinxianren.hadoop.example.top10;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

	// ����һ��TreeMap��Ϊ�洢���ݵ���������Ȼ��key����
	TreeMap<FlowBean, Text> flowMap = new TreeMap<FlowBean, Text>();

	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)throws IOException, InterruptedException {

		for (Text value : values) {

			FlowBean bean = new FlowBean();
			bean.set(key.getDownFlow(), key.getUpFlow());

			// 1 ��treeMap�������������
			flowMap.put(bean, new Text(value));

			// 2 ����TreeMap������������10����ɾ����������С��һ������
			if (flowMap.size() > 10) {
				// flowMap.remove(flowMap.firstKey());
				flowMap.remove(flowMap.lastKey());
			}
		}
	}

	@Override
	protected void cleanup(Reducer<FlowBean, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

		// 3 �������ϣ��������
		Iterator<FlowBean> it = flowMap.keySet().iterator();

		while (it.hasNext()) {

			FlowBean v = it.next();

			context.write(new Text(flowMap.get(v)), v);
		}
	}
}
