package com.yinxianren.hadoop.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class HdfsIO {


	private FileSystem fs=null;
	private Configuration conf=null;
	/**
	 *  init configurration
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{

		conf=new Configuration();
		//1 ��ȡhdfs�ͻ��˶���
		fs= FileSystem.get(new URI("hdfs://192.168.8.158:9000"),conf,"yinxianren");
	}
	/**
	 * �Զ������ļ��ϴ�
	 * @throws IOException
	 */
	@Test
	public void putFileToHDFS() throws IOException{
		long startTime= System.currentTimeMillis();

		//��ȡ������
		FileInputStream fileInputStream=new FileInputStream(new File("D:\\document\\�ο��ĵ�\\�ڿ͹���\\������Ա���Ա��Ա��䡷������ȱ���������е��ҵ�����硿.pdf"));
		//��ȡ�����
		FSDataOutputStream dataOutputStream=fs.create(new Path("/hadoop/demo/������Ա���Ա��Ա��䡷������ȱ���������е��ҵ�����硿.pdf"));
		//��֮�俽��
		IOUtils.copyBytes(fileInputStream, dataOutputStream, conf);

		IOUtils.closeStream(dataOutputStream);
		IOUtils.closeStream(fileInputStream);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
	}

	/**
	 * �ļ�����
	 * @throws IOException
	 */

	@Test
	public void getFileFromHDFS() throws IOException{
		long startTime= System.currentTimeMillis();

		//��ȡ������
		FSDataInputStream dataInputStream=fs.open(new Path("/hadoop/demo/������Ա���Ա��Ա��䡷������ȱ���������е��ҵ�����硿.pdf"));
		//��ȡ�����
		FileOutputStream fileOutputStream=new FileOutputStream(new File("D:/app/������Ա���Ա��Ա��䡷������ȱ���������е��ҵ�����硿.pdf"));
		//��֮�俽��
		IOUtils.copyBytes(dataInputStream, fileOutputStream, conf);

		IOUtils.closeStream(dataInputStream);
		IOUtils.closeStream(fileOutputStream);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
	}

	/**
	 * ��λ�����ļ���һ���
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void readFileSeekOnFirst() throws Exception {
		long startTime= System.currentTimeMillis();

		FSDataInputStream fis=fs.open(new Path("/hadoop/demo/������Ա���Ա��Ա��䡷������ȱ���������е��ҵ�����硿.pdf"));
		FileOutputStream fos=new FileOutputStream(new File("d:/app/����Ա���Ա��Ա���1.pdf"));

		//���������ݣ�ȡ��������
		byte[] buf=new byte[1024];
		for(int i=0;i<1024*128;i++) {
			fis.read(buf);
			fos.write(buf);
		}

		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");

	}
	/**
	 * ��λ�����ļ��ڶ���
	 * 
	 * ��wind�����£���ν��������ݿ�ϲ���һ���ĵ���
	 * ��һ������hadoop��������
	 * �ڶ����ļ����ڵ�Ŀ¼�еĵ�ַ������cmd��
	 * �������������������ƴ����� type ����Ա���Ա��Ա���2.pdf>>����Ա���Ա��Ա���1.pdf
	 * ���ģ��鿴�ļ��Ĵ�С���Լ��ļ��⵽��
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void readFileSeekOnSecond() throws Exception {
		long startTime= System.currentTimeMillis();

		FSDataInputStream fis=fs.open(new Path("/hadoop/demo/������Ա���Ա��Ա��䡷������ȱ���������е��ҵ�����硿.pdf"));
		fis.seek(1024*1024*128);//��λ���ڶ����ݿ�
		FileOutputStream fos=new FileOutputStream(new File("d:/app/����Ա���Ա��Ա���2.pdf"));

		//��֮�俽��
		IOUtils.copyBytes(fis, fos, conf);
		
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");

	}

}
