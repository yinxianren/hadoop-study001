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
		// 配置在集群上运行
		conf.set("fs.defaultFS", "hdfs://192.168.8.158:9000");
		//1 获取hdfs客户端对象
		FileSystem fs= FileSystem.get(conf);
		//2 在hdfs创建目录
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
		//1 获取hdfs客户端对象
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
		System.out.println("操作结果："+how);
	}
	/**
	 * delete direcotory
	 * @throws Exception
	 */
	@Test
	public void del() throws  Exception {

		boolean how=fs.deleteOnExit(new Path("/hadoop/demo/slaves"));
		fs.close();
		System.out.println("操作结果："+how);
	}

	/**
	 * 上传文件
	 * @throws Exception
	 */

	@Test
	public void uploadFiled() throws Exception{
		long startTime= System.currentTimeMillis();
		fs.copyFromLocalFile(new Path("D:\\app\\flightreservation.log"), 
				new Path("/hadoop/demo/"));
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("耗时："+usrTime+"ms");
		fs.close();

	}

	/**
	 *  文件下载
	 * @throws Exception
	 */
	@Test
	public void downloadFiled() throws Exception{
		long startTime= System.currentTimeMillis();
		fs.copyToLocalFile(false, new Path("/hadoop/demo/flightreservation.log"), new Path("D:\\app"), true);
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("耗时："+usrTime+"ms");
		fs.close();

	}

	/**
	 * 文件重名
	 * @throws Exception
	 */
	@Test
	public void rename() throws Exception{
		long startTime= System.currentTimeMillis();
		fs.rename(new Path("/hadoop/demo/flightreservation.log"), new Path("/hadoop/demo/flightreservation-rename.log"));
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("耗时："+usrTime+"ms");
		fs.close();

	}
	/**
	 *  获取文件信息
	 * @throws Exception
	 */
	@Test
	public void getFileInfo() throws Exception{
		long startTime= System.currentTimeMillis();
		RemoteIterator<LocatedFileStatus> iterator=fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()) {
			LocatedFileStatus fileStatus=iterator.next();
			System.out.println("文件名:"+fileStatus.getPath().getName());
			System.out.println("文件的大小:"+fileStatus.getLen());
			System.out.println("文件的权限:"+fileStatus.getPermission());
			System.out.println("文件所属组:"+fileStatus.getGroup());
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
		System.out.println("耗时："+usrTime+"ms");
		fs.close();

	}
	
	/**
	 * 是否是文件/目录
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
		System.out.println("耗时："+usrTime+"ms");
		fs.close();

	}
	
	
	
	
	
	
	
	
	
	
	
}
