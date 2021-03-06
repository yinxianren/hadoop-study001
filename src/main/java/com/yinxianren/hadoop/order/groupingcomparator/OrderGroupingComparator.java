package com.yinxianren.hadoop.order.groupingcomparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
/**
 * 
 * @author root
 *分组排序步骤：
 *（1）自定义类继承WritableComparator
 *（2）重写compare()方法
 *（3）创建一个构造将比较对象的类传给父类
 *
 */
public class OrderGroupingComparator extends WritableComparator {

	protected OrderGroupingComparator() {
		super(OrderBean.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {

		OrderBean aBean = (OrderBean) a;
		OrderBean bBean = (OrderBean) b;

		int result;
		if (aBean.getOrder_id() > bBean.getOrder_id()) {
			result = 1;
		} else if (aBean.getOrder_id() < bBean.getOrder_id()) {
			result = -1;
		} else {
			result = 0;
		}

		return result;
	}
}