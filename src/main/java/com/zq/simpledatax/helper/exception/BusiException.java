package com.zq.simpledatax.helper.exception;

public class BusiException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	
	public BusiException() {
	}
	
	public BusiException(String msg) {
		super(msg);
	}
	public BusiException(String msg,Throwable t) {
		super(msg,t);
	}
}
