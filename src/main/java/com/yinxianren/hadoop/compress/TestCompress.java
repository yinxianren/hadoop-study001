package com.yinxianren.hadoop.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {

	public static void main(String[] args) throws Exception {
		//ѹ��
		compress("e:/hello.txt","org.apache.hadoop.io.compress.BZip2Codec");
		//compress("e:/hello.txt","org.apache.hadoop.io.compress.GzipCodec");
		//compress("e:/hello.txt","org.apache.hadoop.io.compress.DefaultCodec");
		decompress("e:/hello.txt.bz2");//��ѹ
	}

	// 1��ѹ��
	private static void compress(String filename, String method) throws Exception {

		// ��1����ȡ������
		FileInputStream fis = new FileInputStream(new File(filename));

		Class codecClass = Class.forName(method);

		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

		// ��2����ȡ�����
		FileOutputStream fos = new FileOutputStream(new File(filename + codec.getDefaultExtension()));
		CompressionOutputStream cos = codec.createOutputStream(fos);

		// ��3�����ĶԿ�
		IOUtils.copyBytes(fis, cos, 1024*1024*5, false);

		// ��4���ر���Դ
		cos.close();
		fos.close();
		fis.close();
	}

	// 2����ѹ��
	private static void decompress(String filename) throws FileNotFoundException, IOException {

		// ��0��У���Ƿ��ܽ�ѹ��
		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

		CompressionCodec codec = factory.getCodec(new Path(filename));

		if (codec == null) {
			System.out.println("cannot find codec for file " + filename);
			return;
		}

		// ��1����ȡ������
		CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));

		// ��2����ȡ�����
		FileOutputStream fos = new FileOutputStream(new File(filename + ".decoded"));

		// ��3�����ĶԿ�
		IOUtils.copyBytes(cis, fos, 1024*1024*5, false);

		// ��4���ر���Դ
		cis.close();
		fos.close();
	}
}