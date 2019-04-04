package com.zq.simpledatax.plugin.reader.excelreader;

import com.zq.simpledatax.common.spi.ErrorCode;

/**
 * Created by haiwei.luo on 14-9-20.
 */
public enum ExcelReaderErrorCode implements ErrorCode {
	REQUIRED_VALUE("ExcelReader-00", "您缺失了必须填写的参数值."),
	ILLEGAL_VALUE("ExcelReader-01", "您填写的参数值不合法."),
	FILE_NOT_EXISTS("ExcelReader-02", "您配置的目录文件路径不存在."),
	OPEN_FILE_ERROR("ExcelReader-03", "您配置的文件在打开时异常,建议您检查源目录是否有隐藏文件,管道文件等特殊文件."),
	READ_FILE_IO_ERROR("ExcelReader-04", "您配置的文件在读取时出现IO异常."),
	SECURITY_NOT_ENOUGH("ExcelReader-05", "您缺少权限执行相应的文件操作."),
	CONFIG_INVALID_EXCEPTION("ExcelReader-06", "您的参数配置错误."),
	RUNTIME_EXCEPTION("ExcelReader-07", "出现运行时异常, 请联系我们"),
	EMPTY_DIR_EXCEPTION("ExcelReader-08", "您尝试读取的文件目录为空."),
	RECORD_SEND_ERROR("ExcelReader-09", "发送数据时出现异常."),
	FILE_TYPE_ERROR("ExcelReader-10", "文件类型异常.");
	
	private final String code;
	private final String description;

	private ExcelReaderErrorCode(String code, String description) {
		this.code = code;
		this.description = description;
	}

	@Override
	public String getCode() {
		return this.code;
	}

	@Override
	public String getDescription() {
		return this.description;
	}

	@Override
	public String toString() {
		return String.format("Code:[%s], Description:[%s].", this.code,
				this.description);
	}
}
