package com.cimc.sink;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.alibaba.fastjson.JSON;

/**
 * Created by 00013708 on 2017/8/28.
 */
public class Test {

	@org.junit.Test
	public void castTest() {
		// String s = "{\"db_ez.t_parcel\":\"db_ez.t_parcel\"}";
		// Map<String,Object> map = new HashMap<>();
		// //测试integer和long的转换
		// map.put("int",12);
		// map.put("long",12456666666666L);
		// map.put("hello",null);
		// map.put("world","");
		//
		// String hello = StrUtils.valueOf(map.get("hello"));
		// String helloX = (String)map.get("hello");
		//
		//
		// String world = StrUtils.valueOf(map.get("world"));
		// String worldX = (String)map.get("world");
		//
		// String intS = StrUtils.valueOf(map.get("int"));
		// String intX = (String)map.get("int");
		// Object val = map.get("int");
		// if(val instanceof Integer){
		// Integer valStr = Integer.valueOf(StrUtils.valueOf(val));
		// }

	}

	@org.junit.Test
	public void filterTest() {
		Map<String, Map<String, List<Object>>> map = new HashMap<>();
		List<Object> list = new ArrayList<>();
		list.add("100000A011");
		list.add("100000A012");
		list.add("100000A013");

		Map<String, List<Object>> map2 = new HashMap<>();
		map2.put("sn", list);

		map.put("db_ez.t_term", map2);

		String str = JSON.toJSONString(map);
		System.out.println(str);
	}

	@org.junit.Test
	public void equalTest() throws ParseException {
		Map<String, Object> map = new HashMap<>();
		map.put("sn", "518000A123");
		map.put("int", 1000);
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		Date date = sf.parse("2017-08-30 10:10");
		map.put("time", date);
		Map<String, Object> map2 = new HashMap<>();
		map2.put("sn", "518000A123");
		map2.put("long", 1000L);
		map2.put("time", "2017-08-30 10:10");

		boolean equals = map.get("sn").equals(map2.get("sn"));
		Assert.assertTrue(equals);

		boolean equals2 = map.get("int").equals(map2.get("long"));
		Assert.assertFalse(equals2);

		boolean equals3 = map.get("time").equals(map2.get("time"));
		Assert.assertFalse(equals3);

	}
}
