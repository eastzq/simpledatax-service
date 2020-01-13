package com.github.simpledatax.api.adaptor.parser.intf;

import com.github.simpledatax.api.dto.DataCollectPlugin;
import com.github.simpledatax.common.util.Configuration;

public interface Parser {
    
	Configuration parse(DataCollectPlugin plugin);
}
