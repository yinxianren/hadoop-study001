package com.yinxianren.hadoop.reducer.userdefinedpatitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * 
 * @author root
 *  ����ʵ��bean�������л���������7����
��1������ʵ��Writable�ӿ�
��2�������л�ʱ����Ҫ������ÿղι��캯�������Ա����пղι���
��3����д���л�����
��4����д�����л�����
��5��ע�ⷴ���л���˳������л���˳����ȫһ��
��6��Ҫ��ѽ����ʾ���ļ��У���Ҫ��дtoString()�����á�\t���ֿ�����������á�
��7�������Ҫ���Զ����bean����key�д��䣬����Ҫʵ��Comparable�ӿڣ���ΪMapReduce���е�Shuffle����Ҫ���key�������������������������
 */

//1 ʵ��writable�ӿ�
public class FlowBean implements Writable{

	private long upFlow;
	private long downFlow;
	private long sumFlow;

	//2  �����л�ʱ����Ҫ������ÿղι��캯�������Ա�����
	public FlowBean() {
		super();
	}
	
	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}
	
	
	
	//3  д���л�����
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);

	}
	//4 �����л�����
	//5 �����л�������˳������д���л�������д˳�����һ��
	@Override
	public void readFields(DataInput in) throws IOException {

		this.upFlow  = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	
	}

	// 6 ��дtoString���������������ӡ���ı�
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}
	
	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	
	
	
}
