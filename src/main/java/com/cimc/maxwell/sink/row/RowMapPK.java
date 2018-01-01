package com.cimc.maxwell.sink.row;

import com.alibaba.fastjson.JSON;

/**
 * Created by 00013708 on 2017/8/15.
 */
public class RowMapPK {
	private String pk;

	private String pkVal;

	public RowMapPK() {
	}

	public RowMapPK(String pk, String pkVal) {
		this.pk = pk;
		this.pkVal = pkVal;
	}

	public String getPk() {
		return pk;
	}

	public void setPk(String pk) {
		this.pk = pk;
	}

	public String getPkVal() {
		return pkVal;
	}

	public void setPkVal(String pkVal) {
		this.pkVal = pkVal;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}
}
