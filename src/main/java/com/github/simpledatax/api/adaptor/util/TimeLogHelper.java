package com.github.simpledatax.api.adaptor.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeLogHelper {

	private static final String END_TAG = "_end";
	private static final String START_TAG = "_start";
	private static final String TOTAL_TAG = "_total";
	private static Logger logger = LoggerFactory.getLogger(TimeLogHelper.class);
	
	private static boolean isTest = logger.isDebugEnabled();
	
	static {
		if(isTest) {
			local = new InheritableThreadLocal<Map<String, Long>>() {
			    @Override
			    protected Map<String, Long> initialValue() {
			      return new ConcurrentHashMap<String, Long>();
			    }
			};
		}
	}
	
	private static ThreadLocal<Map<String, Long>> local;
	
	public static void start(String key) {
		if(isTest) {
			String startKey = generateStartKey(key);
			long t = System.currentTimeMillis();
			getTimeCache().put(startKey, t);			
		}
	}

	public static void end(String key) {
		if(isTest) {
			String startKey = generateStartKey(key);
			String endKey = generateEndKey(key);
			String totalKey = generateTotalKey(key);
			long startTime = getTimeCache().get(startKey);
			long endTime = System.currentTimeMillis();
			getTimeCache().put(endKey, endTime);
			getTimeCache().put(totalKey, endTime - startTime);
		}
	}

	public static void logAll() {
		if(isTest) {
			for (Map.Entry<String, Long> entry : getTimeCache().entrySet()) {
				if(entry.getKey().contains(TOTAL_TAG)) {
					logger.debug("------------------当前时间类型：{}，时间：{}-----------------", entry.getKey(), entry.getValue());				
				}
			}			
		}
	}

	private static String generateStartKey(String key) {
		String threadName = Thread.currentThread().getName();
		String startKey = key + START_TAG + "_"+threadName;
		return startKey;
	}

	private static String generateEndKey(String key) {
		String threadName = Thread.currentThread().getName();
		String endKey = key + END_TAG +"_"+threadName;
		return endKey;
	}

	private static String generateTotalKey(String key) {
		String threadName = Thread.currentThread().getName();
		String totalKey = key + TOTAL_TAG + "_"+threadName;
		return totalKey;
	}

	public static void logKey(String key) {
		for (Map.Entry<String, Long> entry : getTimeCache().entrySet()) {
			if (entry.getKey().startsWith(key)) {
				logger.debug("------------------当前时间类型：{}，时间：{}-----------------", entry.getKey(), entry.getValue());
				return;
			}
		}
	}

	
	private static Map<String, Long> getTimeCache(){
		return local.get();
	}
}
