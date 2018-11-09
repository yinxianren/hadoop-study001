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
		//1 获取hdfs客户端对象
		fs= FileSystem.get(new URI("hdfs://192.168.8.158:9000"),conf,"yinxianren");
	}
	/**
	 * 自定流，文件上传
	 * @throws IOException
	 */
	@Test
	public void putFileToHDFS() throws IOException{
		long startTime= System.currentTimeMillis();

		//获取输入流
		FileInputStream fileInputStream=new FileInputStream(new File("D:\\document\\参考文档\\黑客攻防\\《程序员面试笔试宝典》（何吴等编著）【机械工业出版社】.pdf"));
		//获取输出流
		FSDataOutputStream dataOutputStream=fs.create(new Path("/hadoop/demo/《程序员面试笔试宝典》（何吴等编著）【机械工业出版社】.pdf"));
		//流之间拷贝
		IOUtils.copyBytes(fileInputStream, dataOutputStream, conf);

		IOUtils.closeStream(dataOutputStream);
		IOUtils.closeStream(fileInputStream);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("耗时："+usrTime+"ms");
	}

	/**
	 * 文件下载
	 * @throws IOException
	 */

	@Test
	public void getFileFromHDFS() throws IOException{
		long startTime= System.currentTimeMillis();

		//获取输入流
		FSDataInputStream dataInputStream=fs.open(new Path("/hadoop/demo/《程序员面试笔试宝典》（何吴等编著）【机械工业出版社】.pdf"));
		//获取输出流
		FileOutputStream fileOutputStream=new FileOutputStream(new File("D:/app/《程序员面试笔试宝典》（何吴等编著）【机械工业出版社】.pdf"));
		//流之间拷贝
		IOUtils.copyBytes(dataInputStream, fileOutputStream, conf);

		IOUtils.closeStream(dataInputStream);
		IOUtils.closeStream(fileOutputStream);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("耗时："+usrTime+"ms");
	}

	/**
	 * 定位下载文件第一块块
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void readFileSeekOnFirst() throws Exception {
		long startTime= System.currentTimeMillis();

		FSDataInputStream fis=fs.open(new Path("/hadoop/demo/《程序员面试笔试宝典》（何吴等编著）【机械工业出版社】.pdf"));
		FileOutputStream fos=new FileOutputStream(new File("d:/app/程序员面试笔试宝典1.pdf"));

		//读多少数据，取多少数据
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
		System.out.println("耗时："+usrTime+"ms");

	}
	/**
	 * 定位下载文件第二块
	 * 
	 * 在wind环境下，如何将两块数据块合并成一个文档？
	 * 第一：配置hadoop环境变量
	 * 第二：文件所在的目录中的地址栏输入cmd，
	 * 第三：在命令窗口中输入拼接命令： type 程序员面试笔试宝典2.pdf>>程序员面试笔试宝典1.pdf
	 * 第四：查看文件的大小，以及文件遭到损坏
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void readFileSeekOnSecond() throws Exception {
		long startTime= System.currentTimeMillis();

		FSDataInputStream fis=fs.open(new Path("/hadoop/demo/《程序员面试笔试宝典》（何吴等编著）【机械工业出版社】.pdf"));
		fis.seek(1024*1024*128);//定位到第二数据块
		FileOutputStream fos=new FileOutputStream(new File("d:/app/程序员面试笔试宝典2.pdf"));

		//流之间拷贝
		IOUtils.copyBytes(fis, fos, conf);
		
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);


		fs.close();
		long endTime=System.currentTimeMillis();
		long usrTime=endTime-startTime;
		System.out.println("耗时："+usrTime+"ms");

	}

}
