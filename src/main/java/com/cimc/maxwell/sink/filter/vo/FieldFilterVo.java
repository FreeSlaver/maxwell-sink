package com.cimc.maxwell.sink.filter.vo;

/**
 * Created by 00013708 on 2017/9/11.
 */
public class FieldFilterVo {

	private String database;

	private String table;

	private FieldFilterDtlVo fieldCondition;

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public FieldFilterDtlVo getFieldCondition() {
		return fieldCondition;
	}

	public void setFieldCondition(FieldFilterDtlVo fieldCondition) {
		this.fieldCondition = fieldCondition;
	}
}
