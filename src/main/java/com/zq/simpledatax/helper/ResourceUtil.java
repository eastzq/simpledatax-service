package com.zq.simpledatax.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.pagehelper.Dialect;

/**
 * 加载资源文件
 * 
 * @author zq
 */
public class ResourceUtil {
	static Logger logger = LoggerFactory.getLogger(ResourceUtil.class);

	/**
	 * 获取jar包中的资源文件，可以解决不同jar包，配置文件名字一样加载的问题。
	 * 
	 * @param clazz
	 *            jar包中的任一Class对象
	 * @param resourceName
	 *            资源名称 如a.xml 也可以是a/b.xml classpath下
	 * @return 框架都支持输入流加载配置文件。
	 * @throws Exception
	 */
	public static InputStream getResourceInJar(Class clazz, String resourceName) throws Exception {
		String filePath = getJarPath(clazz);
		InputStream in = null;
		File file = new File(filePath);
		if (filePath.endsWith(".jar") && !file.isDirectory()) {
			URL resourceUrl = new URL("jar:file:" + filePath + "!/" + resourceName);
			JarURLConnection jarConnection = (JarURLConnection) resourceUrl.openConnection();
			in = jarConnection.getInputStream();
		} else {
			in = new FileInputStream(new File(filePath + "/" + resourceName));
		}
		return in;
	}

	
	public static String getJarPath(Class clazz) throws Exception {
		URL url = clazz.getProtectionDomain().getCodeSource().getLocation();
		String filePath = URLDecoder.decode(url.getPath(), "utf-8");
		return filePath;
	}
	/**
	 * 将jar包中的资源文件复制到指定目录下，jar包中资源文件的目录结构保持不变！ 如果目标文件存在，则忽略，如果不存在，则创建。不覆盖。
	 * 
	 * @param clazz
	 *            jar包中的任一Class对象
	 * @param resourceName
	 *            资源名称 如a.xml 也可以是a/b.xml classpath下
	 * @return void
	 * @throws Exception 
	 */
	public static void copyRecourceFromJar(Class clazz, String resourceName, String destFolderPath,boolean isOverride)
			throws Exception {
		String filePath = getJarPath(clazz);
		URL resourceUrl = new URL("jar:file:" + filePath + "!/" + resourceName);
		JarURLConnection jarUrlConnection = (JarURLConnection) resourceUrl.openConnection();
		JarFile jarFile = jarUrlConnection.getJarFile();
		Enumeration<JarEntry> entrys = jarFile.entries();
		while (entrys.hasMoreElements()) {
			JarEntry entry = entrys.nextElement();
			if (entry.getName().startsWith(jarUrlConnection.getEntryName()) && !entry.getName().endsWith("/")) {
				loadRecourceFromJar("/" + entry.getName(), destFolderPath, clazz, isOverride);
			}
		}
		jarFile.close();
	}

	private static void loadRecourceFromJar(String path, String destFolderPath, Class clazz,boolean isOverride) throws IOException {
		if (!path.startsWith("/")) {
			throw new IllegalArgumentException("The path has to be absolute (start with '/').");
		}
		if (path.endsWith("/")) {
			throw new IllegalArgumentException("The path has to be absolute (cat not end with '/').");
		}
		int index = path.lastIndexOf('/');
		String filename = path.substring(index + 1);
		String folderPath = destFolderPath + path.substring(0, index + 1);
		// If the folder does not exist yet, it will be created. If the folder
		// exists already, it will be ignored
		File dir = new File(folderPath);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		// If the file does not exist yet, it will be created. If the file
		// exists already, it will be ignored
		filename = folderPath + filename;
		File file = new File(filename);
		if (file.exists() && isOverride) {
			logger.warn(" 删除文件:{}",filename);
			file.delete();
		}else if(file.exists()){
			return;
		}
		// Prepare buffer for data copying
		byte[] buffer = new byte[1024];
		int readBytes;

		// Open and check input stream
		URL url = clazz.getResource(path);
		URLConnection urlConnection = url.openConnection();
		InputStream is = urlConnection.getInputStream();
		if (is == null) {
			throw new FileNotFoundException("File " + path + " was not found inside JAR.");
		}
		OutputStream os = new FileOutputStream(file);
		try {
			while ((readBytes = is.read(buffer)) != -1) {
				os.write(buffer, 0, readBytes);
			}
		} finally {
			os.close();
			is.close();
		}
	}

	/**
	 * 删除文件，如果是文件夹则递归删除文件夹内的所有内容并删除文件。
	 * @param file 文件对象
	 * @throws Exception 文件不存在时异常，或者文件为空
	 */
	public static void deleteFile(File file) throws Exception {
		if(file==null && !file.exists()) {
		}else {
			if (file.isFile()) {
				file.delete();
			} else if (file.isDirectory()) {
				File[] files = file.listFiles();
				for (int i = 0; i < files.length; i++) {
					deleteFile(files[i]);
				}
				file.delete();
			}
		}
	}
	public static void main(String[] args) throws Exception {
		copyRecourceFromJar(Dialect.class, "META-INF", "D:/test1",true);
//		getResourceInJar(Dialect.class,"META-INF");
	}

}
