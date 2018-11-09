package com.yinxianren.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient {


	@Test
	public void test001() throws IOException {
		Configuration conf=new Configuration();
		// �����ڼ�Ⱥ������
		conf.set("fs.defaultFS", "hdfs://192.168.8.158:9000");
		//1 ��ȡhdfs�ͻ��˶���
		FileSystem fs= FileSystem.get(conf);
		//2 ��hdfs����Ŀ¼
		fs.mkdirs(new Path("/hadoop/demo/"));
		fs.close();
		System.out.println("over!");
	}

	private FileSystem fs=null;
	/**
	 *  init configurration
	 * @throws Exception
	 */
	@Before
	public void init() throws Exception{

		Configuration conf=new Configuration();
		//1 ��ȡhdfs�ͻ��˶���
		fs= FileSystem.get(new URI("hdfs://192.168.8.158:9000"),conf,"yinxianren");
	}
	/**
	 * create new direcotory
	 * @throws Exception
	 */
	@Test
	public void mkdir() throws  Exception {

		boolean how=fs.mkdirs(new Path("/hadoop/demo/"));
		fs.close();
		System.out.println("���������"+how);
	}
	/**
	 * delete direcotory
	 * @throws Exception
	 */
	@Test
	public void del() throws  Exception {

		boolean how=fs.deleteOnExit(new Path("/hadoop/demo/slaves"));
		fs.close();
		System.out.println("���������"+how);
	}

	/**
	 * �ϴ��ļ�
	 * @throws Exception
	 */

	@Test
	public void uploadFiled() throws Exception{
		long startTime= System.currentTimeMillis();
		fs.copyFromLocalFile(new Path("D:\\app\\flightreservation.log"), 
				new Path("/hadoop/demo/"));
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
		fs.close();

	}

	/**
	 *  �ļ�����
	 * @throws Exception
	 */
	@Test
	public void downloadFiled() throws Exception{
		long startTime= System.currentTimeMillis();
		fs.copyToLocalFile(false, new Path("/hadoop/demo/flightreservation.log"), new Path("D:\\app"), true);
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
		fs.close();

	}

	/**
	 * �ļ�����
	 * @throws Exception
	 */
	@Test
	public void rename() throws Exception{
		long startTime= System.currentTimeMillis();
		fs.rename(new Path("/hadoop/demo/flightreservation.log"), new Path("/hadoop/demo/flightreservation-rename.log"));
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
		fs.close();

	}
	/**
	 *  ��ȡ�ļ���Ϣ
	 * @throws Exception
	 */
	@Test
	public void getFileInfo() throws Exception{
		long startTime= System.currentTimeMillis();
		RemoteIterator<LocatedFileStatus> iterator=fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()) {
			LocatedFileStatus fileStatus=iterator.next();
			System.out.println("�ļ���:"+fileStatus.getPath().getName());
			System.out.println("�ļ��Ĵ�С:"+fileStatus.getLen());
			System.out.println("�ļ���Ȩ��:"+fileStatus.getPermission());
			System.out.println("�ļ�������:"+fileStatus.getGroup());
			BlockLocation[]  blockLocations= fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts=blockLocation.getHosts();
				for (String host : hosts) {
					System.out.println("ip:"+host);
				}
				
				String[] topologyPaths=blockLocation.getTopologyPaths();
				for(String topologyPath:topologyPaths) {
					System.out.println(topologyPath);
				}
				
				String[] names=blockLocation.getNames();
                 for(String name:names) {
                	 System.out.println(name);
                 }
			}

			System.out.println("*****************************");
		}

		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
		fs.close();

	}
	
	/**
	 * �Ƿ����ļ�/Ŀ¼
	 * @throws Exception
	 */
	@Test
	public void whetherDiretory() throws Exception{
		long startTime= System.currentTimeMillis();
		FileStatus[] fileStatus=fs.listStatus(new Path("/hadoop/demo/"));
		for(FileStatus fileStatu:fileStatus) {
			if(fileStatu.isDirectory()) {
				System.out.println("diretory:"+fileStatu.getPath().getName());
			}else {
				System.out.println("file:"+fileStatu.getPath().getName());
			}
		}
		
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("��ʱ��"+usrTime+"ms");
		fs.close();

	}
	
	
	
	
	
	
	
	
	
	
	
}
