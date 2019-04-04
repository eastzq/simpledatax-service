package com.zq.simpledatax.api.message;

public enum ReadWriteMode {
    JDBC("jdbc"), FILE("file"),DBLINK("dblink");

    private String mode;

    ReadWriteMode(String mode) {
        this.mode = mode;
    }

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}
	
}
