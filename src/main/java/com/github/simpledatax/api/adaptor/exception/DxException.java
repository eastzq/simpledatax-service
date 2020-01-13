package com.github.simpledatax.api.adaptor.exception;

public class DxException extends RuntimeException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	
	
	public DxException() {
	}
	
	public DxException(String msg) {
		super(msg);
	}
	public DxException(String msg,Throwable t) {
		super(msg,t);
	}
}
