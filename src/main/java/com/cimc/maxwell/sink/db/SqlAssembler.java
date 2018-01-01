package com.cimc.maxwell.sink.db;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.cimc.maxwell.sink.filter.MatchCase;
import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;
import com.cimc.maxwell.sink.util.StrUtils;

public class SqlAssembler {
	public String getSql(MatchCase matchCase, RowMapPK rowMapPK, RowMap rowMap) {
		if (matchCase == MatchCase.MATCH) {
			if (rowMapPK == null) {
				return getUpdateSql(rowMapPK, rowMap);
			} else {
				return getInsertSql(rowMap);
			}
		} else if (matchCase == MatchCase.NOMATCH) {
			return getDeleteSql(rowMapPK, rowMap);
		}
		return "";
	}

	private String getDeleteSql(RowMapPK rowMapPK, RowMap rowMap) {
		if (rowMap == null) {
			return "";
		}
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		Map<String, Object> data = rowMap.getData();
		String dbTableWrapped = wrapDT(database, table);

		StringBuilder sqlSb = new StringBuilder();
		/** DELETE maxwell生成的JSON中只有data有值 **/
		sqlSb.append("DELETE FROM ").append(dbTableWrapped);
		sqlSb.append(" ").append(getWhereClause(rowMapPK, data));
		return sqlSb.toString();
	}

	private String getUpdateSql(RowMapPK rowMapPK, RowMap rowMap) {
		if (rowMap == null) {
			return null;
		}
		Map<String, Object> old = rowMap.getOld();
		if (old == null || old.isEmpty()) {
			return null;
		}
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		Map<String, Object> data = rowMap.getData();
		String dbTableWrapped = wrapDT(database, table);

		StringBuilder sqlSb = new StringBuilder();
		sqlSb.append("UPDATE ").append(dbTableWrapped);
		sqlSb.append(" SET ");
		for (Map.Entry<String, Object> entry : old.entrySet()) {
			String key = entry.getKey();
			// 得到新值
			String val = StrUtils.escapeSql(data.get(key));
			sqlSb.append(wrapKV(key, val)).append(",");
		}
		sqlSb.deleteCharAt(sqlSb.lastIndexOf(","));
		/** UPDATE 通过data和old，全量字段匹配得到之前的唯一结果 **/
		data.putAll(old);

		sqlSb.append(" ").append(getWhereClause(rowMapPK, data));
		return sqlSb.toString();
	}

	private String getInsertSql(RowMap rowMap) {
		if (rowMap == null) {
			return null;
		}
		// String type = rowMap.getType();
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		Map<String, Object> data = rowMap.getData();
		String dbTableWrapped = wrapDT(database, table);

		StringBuilder sqlSb = new StringBuilder();
		sqlSb.append("INSERT INTO ").append(dbTableWrapped);
		StringBuilder keySb = new StringBuilder("(");
		StringBuilder valSb = new StringBuilder("(");
		for (Map.Entry<String, Object> entry : data.entrySet()) {
			String key = entry.getKey();
			String val = StrUtils.escapeSql(entry.getValue());
			keySb.append("`").append(key).append("`").append(",");
			if (val == null) {// 为null的忽略掉
				valSb.append(val).append(",");
			} else {
				valSb.append("'").append(val).append("'").append(",");
			}
		}
		keySb.deleteCharAt(keySb.lastIndexOf(","));
		valSb.deleteCharAt(valSb.lastIndexOf(","));
		keySb.append(")");
		valSb.append(")");

		sqlSb.append(" ").append(keySb).append(" VALUES ").append(valSb);
		// insert or update
		sqlSb.append(" ON DUPLICATE KEY UPDATE ");
		for (Map.Entry<String, Object> entry : data.entrySet()) {
			String key = entry.getKey();
			String val = StrUtils.escapeSql(data.get(key));
			sqlSb.append(wrapKV(key, val)).append(",");
		}
		sqlSb.deleteCharAt(sqlSb.lastIndexOf(","));

		return sqlSb.toString();
	}

	public String getSelectSql(RowMapPK rowMapPK, RowMap rowMap) {
		if (rowMap == null) {
			return null;
		}
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		Map<String, Object> data = rowMap.getData();
		Map<String, Object> old = rowMap.getOld();
		String dbTableWrapped = wrapDT(database, table);

		StringBuilder sqlSb = new StringBuilder();
		/** 必须要唯一主键，不然这种查询返回的都有值 **/
		sqlSb.append("SELECT * FROM ").append(dbTableWrapped);

		/** 应对无主键的情况，将旧值放入data进行查询 **/
		if (old != null && !old.isEmpty()) {
			data.putAll(old);
		}
		sqlSb.append(" ").append(getWhereClause(rowMapPK, data));
		return sqlSb.toString();
	}

	/**
	 * 根据pk或者旧值组装where条件子句 ###注意：data是将old放入data后的值，type是delete的没有old
	 *
	 * @param rowMapPK
	 * @param data
	 * @return
	 */
	private String getWhereClause(RowMapPK rowMapPK, Map<String, Object> data) {
		StringBuilder sb = new StringBuilder("WHERE ");
		if (rowMapPK != null) {
			String pk = rowMapPK.getPk();
			String pkVal = rowMapPK.getPkVal();
			sb.append(wrapKV(pk, pkVal));
			return sb.toString();
		} else {
			boolean firstKV = true;
			for (Map.Entry<String, Object> entry : data.entrySet()) {
				String key = entry.getKey();
				String val = StrUtils.escapeSql(entry.getValue());
				if (firstKV) {
					sb.append(wrapKV(key, val));
					firstKV = false;
				} else {
					sb.append(" AND ").append(wrapKV(key, val));
				}
			}
			return sb.toString();
		}
	}

	private String wrapKV(String key, String val) {
		if (StringUtils.isEmpty(key)) {
			return null;
		}
		/** val可以为null，因为有些sql将字段的值更新为null **/
		StringBuilder sb = new StringBuilder();
		sb.append("`").append(key).append("`").append("=");
		if (val == null) {
			sb.append(val);
		} else {
			sb.append("'").append(val).append("'");
		}
		return sb.toString();
	}

	private String wrapDT(String database, String table) {
		if (StringUtils.isEmpty(database) || StringUtils.isEmpty(table)) {
			return null;
		}
		return new StringBuffer().append("`").append(database).append("`").append(".").append("`").append(table)
				.append("`").toString();
	}
}
