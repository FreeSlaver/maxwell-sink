package com.cimc.maxwell.sink.row;

import java.util.Arrays;
import java.util.List;

/**
 * Created by 00013708 on 2017/8/10.
 */
public class RowMapTypes {

	private static final List<String> DDLTypes = Arrays.asList("database-create", "database-alter", "database-drop",
			"table-create", "table-alter", "table-drop");
	private static final List<String> DMLTypes = Arrays.asList("insert", "update", "delete");

	public static boolean isDDL(String type) {
		return DDLTypes.contains(type.toLowerCase());
	}

	public static boolean isDML(String type) {
		return DMLTypes.contains(type.toLowerCase());
	}

}
