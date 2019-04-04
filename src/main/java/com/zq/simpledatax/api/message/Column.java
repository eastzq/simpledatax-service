package com.zq.simpledatax.api.message;

import java.io.Serializable;

public class Column implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /** 字段名 */
    private String name;

    /** 字段类型：N-数字 C-字符串 D-日期 */
    private String type;

    /** 长度，包括小数位长度，不包括小数点 */
    private int length;

    /** 小数位长度 */
    private int decimal;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getDecimal() {
        return decimal;
    }

    public void setDecimal(int decimal) {
        this.decimal = decimal;
    }

}
