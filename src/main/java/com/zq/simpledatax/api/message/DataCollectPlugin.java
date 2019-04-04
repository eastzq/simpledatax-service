package com.zq.simpledatax.api.message;

import java.io.Serializable;

public interface DataCollectPlugin extends Serializable {
	PluginType getPluginType ();
	String getPluginKey();
}
