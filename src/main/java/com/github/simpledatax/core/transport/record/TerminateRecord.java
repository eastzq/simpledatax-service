package com.github.simpledatax.core.transport.record;

import com.github.simpledatax.common.element.Column;
import com.github.simpledatax.common.element.Record;

/**
 * 作为标示 生产者已经完成生产的标志
 * 
 */
public class TerminateRecord implements Record {
	private final static TerminateRecord SINGLE = new TerminateRecord();

	private TerminateRecord() {
	}

	public static TerminateRecord get() {
		return SINGLE;
	}

	@Override
	public void addColumn(Column column) {
	}

	@Override
	public Column getColumn(int i) {
		return null;
	}

	@Override
	public int getColumnNumber() {
		return 0;
	}

	@Override
	public int getByteSize() {
		return 0;
	}

	@Override
	public int getMemorySize() {
		return 0;
	}

	@Override
	public void setColumn(int i, Column column) {
		return;
	}
}
