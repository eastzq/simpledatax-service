package com.zq.simpledatax.api.message;

public enum PluginType {
	READER("reader"),WRITER("writer"),DB("db"),FILE("file");
	private PluginType(String type) {
		this.type=type;
	}
	private String type;

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
}
