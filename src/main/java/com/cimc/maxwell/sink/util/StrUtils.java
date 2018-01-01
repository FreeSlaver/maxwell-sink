package com.cimc.maxwell.sink.util;

/**
 * Created by 00013708 on 2017/8/29.
 */
public class StrUtils {

	public static String valueOf(Object obj) {
		return (obj == null) ? null : obj.toString();
	}

	public static String escapeSql(Object val) {
		return val == null ? null : val.toString().replace("'", "''");
	}
}
