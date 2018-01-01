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
 * Created by 00013708 on 2017/8/16. 根据topic设置的目标database，将rowMap中的database进行重定向
 */
public class DbTableReplacer {
	private static final Logger log = LoggerFactory.getLogger(DbTableReplacer.class);
	private final Map<String, String> dbTableMap;

	public DbTableReplacer(final String dbtables) {
		this.dbTableMap = initDbTableMap(dbtables);
	}

	private Map<String, String> initDbTableMap(String dbtables) {
		if (StringUtils.isEmpty(dbtables)) {
			log.warn("dbtables config null");
			return Collections.emptyMap();
		}
		log.info("===>>>initDbTableMap,values:{}", dbtables);
		return JSON.parseObject(dbtables, new TypeReference<Map<String, String>>() {
		}.getType());
	}

	public RowMap replace(RowMap rowMap) {
		if (rowMap == null) {
			return null;
		}
		if (dbTableMap == null || dbTableMap.isEmpty()) {
			return rowMap;
		}
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		String originDbTable = database + "." + table;
		// 判断是否要根据topic替换目标数据库
		if (dbTableMap.containsKey(originDbTable)) {
			String destDbTable = dbTableMap.get(originDbTable);
			String[] arr = destDbTable.split("\\.");
			String destDb = arr[0];
			String destTable = arr[1];
			log.info("===>>>Replace originDbTable:{} to destDbTable:{}", originDbTable, destDbTable);
			rowMap.setDatabase(destDb);
			rowMap.setTable(destTable);
		}
		return rowMap;
	}
}
