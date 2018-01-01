package com.cimc.sink.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import com.cimc.maxwell.sink.db.SqlAssembler;
import com.cimc.maxwell.sink.filter.MatchCase;
import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;

/**
 * Created by 00013708 on 2017/8/8.
 */
public class SqlAssemblerTest {

	@Test
	public void getSqlTest() {
		SqlAssembler assembler = new SqlAssembler();
		RowMap updateRow = updateRow();
		RowMapPK rowMapPK = new RowMapPK();
		rowMapPK.setPk("id");
		rowMapPK.setPkVal("");
		System.out.println(assembler.getSql(MatchCase.MATCH, rowMapPK, updateRow));
	}

	@Test
	public void getWhereClauseTest() {
		RowMapPK rowMapPK = new RowMapPK();
		rowMapPK.setPk("_uuid");
		rowMapPK.setPkVal("100000A01120160629150388");
	}

	@Test
	public void noPKTest() {
		SqlAssembler assembler = new SqlAssembler();
		RowMapPK rowMapPK = new RowMapPK();
		rowMapPK.setPk("_uuid");
		rowMapPK.setPkVal("100000A01120160629150388");
		RowMap updateRow = updateRow();
		System.out.println(assembler.getSql(MatchCase.MATCH, rowMapPK, updateRow));
	}

	private RowMap updateRow() {
		RowMap rowMap = new RowMap();
		rowMap.setDatabase("db_ez");
		rowMap.setTable("t_parcel");
		rowMap.setType("update");

		Map<String, Object> data = new HashMap<>();
		// data.put("id","100000A01120160629150388");
		data.put("status", "book");
		data.put("package_id", "sx8888");
		data.put("pickup_mobile", "1802536");
		rowMap.setData(data);

		Map<String, Object> old = new HashMap<>();
		old.put("status", "cancel");
		old.put("package_id", "sx6666");
		old.put("pickup_mobile", null);
		rowMap.setOld(old);
		return rowMap;
	}

	// private RowMap deleteRow() {
	// RowMap rowMap = new RowMap();
	// rowMap.setDatabase("db_ez");
	// rowMap.setTable("t_parcel");
	// rowMap.setType("delete");
	//
	// Map<String,Object> data = new HashMap<>();
	//// data.put("id","100000A01120160629150388");
	// data.put("status","book");
	// data.put("package_id","sx8888");
	// data.put("pickup_mobile","");
	// rowMap.setData(data);
	//
	// Map<String,Object> old = new HashMap<>();
	// old.put("status","cancel");
	// old.put("package_id","sx6666");
	// old.put("pickup_mobile",null);
	// rowMap.setOld(old);
	// return rowMap;
	//
	// }
	@Test
	public void getInsertSql() {
		// StringUtils.join(data.keySet(),"`,`");
		// StringUtils.join(data.values(),"','");
		Map<String, Object> map = new HashMap<>();
		map.put("id", "54646464121");
		map.put("box_id", "sdf4s6465");
		map.put("sn", "5180000A5636");
		map.put("type", "small");

		// for(Map.Entry<String,String> entry:map.entrySet()){
		// String key =
		// }
		Set<String> keySet = map.keySet();
		Collection<Object> values = map.values();

		String keyStr = StringUtils.join(keySet, "`,`");
		String valStr = StringUtils.join(values, "`,`");
		System.out.println(keyStr);
		System.out.println(valStr);

		String warpped = "`" + "hello" + "`";
		System.out.println(warpped);

		// 对，可以这么玩，将一个的所有元素遍历，然后wrap。wrap完了之后，使用join

		/*
		 * box_id`,`id`,`sn`,`type
		 * sdf4s6465`,`54646464121`,`5180000A5636`,`small `hello`
		 * `box_id`,`id`,`sn`,`type`
		 */

		// SqlAssembler assembler = new SqlAssembler();
		// String result = assembler.wrapAndJoin(keySet,"`",",");
		// System.out.println(result);
		List<String> list = new ArrayList<>();
		list.add("hello");
		list.add(null);
		System.out.println(Arrays.toString(list.toArray()));
	}
}
