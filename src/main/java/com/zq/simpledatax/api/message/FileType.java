package com.zq.simpledatax.api.message;

public enum FileType {

    DBF("dbf"), BCP("bcp"), CSV("csv"), FUNDTXT("fundtxt"), FUNDTXT_SHJJT("fundtxt_shjjt");

    private String fileType;

    public String getFileType() {
        return this.fileType;
    }

    FileType(String fileType) {
        this.fileType = fileType;
    }

    public FileType valueof(String fileType) {
        if (fileType == null || fileType.length() == 0) {
            return null;
        }
        
        for (FileType em : values()) {
            if (em.getFileType().equals(fileType)) {
                return em;
            }
        }
        return null;
    }
}
