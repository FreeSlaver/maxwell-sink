package com.cimc.maxwell.sink.filter;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cimc.maxwell.sink.row.RowMap;

/**
 * Created by 00013708 on 2017/8/30.
 */
public class DMLFilter implements Filter {
	private static final Logger log = LoggerFactory.getLogger(DMLFilter.class);
	private final Map<String, String> dmlFilterMap;

	public DMLFilter(final String dmlFilter) {
		this.dmlFilterMap = initDMLFiltersMAP(dmlFilter);
	}

	private Map<String, String> initDMLFiltersMAP(String dmlFilter) {
		if (StringUtils.isEmpty(dmlFilter)) {
			log.warn("DML filter config null");
			return Collections.emptyMap();
		}
		log.info("initDmlFiltersMap,values:{}", dmlFilter);
		return JSON.parseObject(dmlFilter, new TypeReference<Map<String, String>>() {
		}.getType());
	}

	public boolean match(RowMap rowMap) {
		if (rowMap == null) {
			return false;
		}
		if (dmlFilterMap == null || dmlFilterMap.isEmpty()) {
			return true;
		}
		String type = rowMap.getType();
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		String dbTable = database + "." + table;
		// 不需要进行dml操作过滤
		if (!dmlFilterMap.containsKey(dbTable)) {
			return true;
		}
		String dmlType = dmlFilterMap.get(dbTable);
		if (StringUtils.isNotEmpty(dmlType) && dmlType.contains(type)) {
			return true;
		} else {
			log.info("DMLFilter filter out RowMap:{}", rowMap.toString());
			return false;
		}
	}
}
