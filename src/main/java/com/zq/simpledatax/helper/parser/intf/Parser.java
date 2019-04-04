package com.zq.simpledatax.helper.parser.intf;

import com.zq.simpledatax.api.message.DataCollectPlugin;
import com.zq.simpledatax.common.util.Configuration;
import com.zq.simpledatax.helper.exception.BusiException;

public interface Parser {
	Configuration parse(DataCollectPlugin plugin) throws BusiException;
}
