package com.github.simpledatax.plugin.reader.excelreader;

public class DataSheet {
	private int sheetIndex = 1;
	private int skipRowNum = 0;
	
	public DataSheet(int sheetIndex,int skipRowNum){
		this.skipRowNum = skipRowNum;
		this.sheetIndex = sheetIndex;
	}
	public DataSheet() {}
	public int getSheetIndex() {
		return sheetIndex;
	}
	public void setSheetIndex(int sheetIndex) {
		this.sheetIndex = sheetIndex;
	}
	public int getSkipRowNum() {
		return skipRowNum;
	}
	public void setSkipRowNum(int skipRowNum) {
		this.skipRowNum = skipRowNum;
	}
}
