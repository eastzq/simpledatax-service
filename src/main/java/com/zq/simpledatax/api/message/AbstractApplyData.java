package com.zq.simpledatax.api.message;

import java.io.Serializable;

public abstract class AbstractApplyData implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * 来源标识
     */
    private String originFlag;

    public String getOriginFlag() {
        return originFlag;
    }

    public void setOriginFlag(String originFlag) {
        this.originFlag = originFlag;
    }

}
