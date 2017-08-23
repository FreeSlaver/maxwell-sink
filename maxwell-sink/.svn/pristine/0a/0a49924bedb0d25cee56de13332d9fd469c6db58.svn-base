package com.cimc.maxwell.sink.db;

import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by 00013708 on 2017/8/7.
 */
public class SqlAssembler {
    private static final Logger log = LoggerFactory.getLogger(SqlAssembler.class);

    public SqlAssembler() {

    }

    public String getSql(RowMapPK rowMapPK, RowMap rowMap) {
        if (rowMap == null) {
            return null;
        }
        String type = rowMap.getType();
        if (type.equalsIgnoreCase("INSERT")) {
            String insertSql = getInsertSql(rowMap);
            return insertSql;
        } else if (type.equalsIgnoreCase("UPDATE")) {
            String updateSql = getUpdateSql(rowMapPK, rowMap);
            return updateSql;

        } else if (type.equalsIgnoreCase("DELETE")) {
            String deleteSql = getDeleteSql(rowMapPK, rowMap);
            return deleteSql;
        } else {
            return null;
        }
    }

    private String getDeleteSql(RowMapPK rowMapPK, RowMap rowMap) {
        if (rowMap == null) {
            return null;
        }
        String type = rowMap.getType();
        if(!type.equalsIgnoreCase("DELETE")){
            return null;
        }
        String database = rowMap.getDatabase();
        String table = rowMap.getTable();
        Map<String, String> data = rowMap.getData();
        String dbTableWrapped = wrapDT(database, table);

        StringBuilder sqlSb = new StringBuilder();
        /**DELETE maxwell生成的JSON中只有data有值**/
        sqlSb.append("DELETE FROM ").append(dbTableWrapped);
        sqlSb.append(" ").append(getWhereClause(rowMapPK, data));
        return sqlSb.toString();
    }

    private String getUpdateSql(RowMapPK rowMapPK, RowMap rowMap) {
        if (rowMap == null) {
            return null;
        }
        String type = rowMap.getType();
        if(!type.equalsIgnoreCase("UPDATE")){
            return null;
        }
        Map<String, String> old = rowMap.getOld();
        if (old == null || old.isEmpty()) {
            return null;
        }
        String database = rowMap.getDatabase();
        String table = rowMap.getTable();
        Map<String, String> data = rowMap.getData();
        String dbTableWrapped = wrapDT(database, table);

        StringBuilder sqlSb = new StringBuilder();
        sqlSb.append("UPDATE ").append(dbTableWrapped);
        sqlSb.append(" SET ");
        for (Map.Entry<String, String> entry : old.entrySet()) {
            String key = entry.getKey();
            //得到新值
            String val = data.get(key);
            sqlSb.append(wrapKV(key, val)).append(",");
        }
        sqlSb.deleteCharAt(sqlSb.lastIndexOf(","));
        /**UPDATE 通过data和old，全量字段匹配得到之前的唯一结果**/
        data.putAll(old);

        sqlSb.append(" ").append(getWhereClause(rowMapPK, data));
        return sqlSb.toString();
    }


    private String getInsertSql(RowMap rowMap) {
        if (rowMap == null) {
            return null;
        }
        String type = rowMap.getType();
        if(!type.equalsIgnoreCase("INSERT")){
            return null;
        }
        String database = rowMap.getDatabase();
        String table = rowMap.getTable();
        Map<String, String> data = rowMap.getData();
        String dbTableWrapped = wrapDT(database, table);

        StringBuilder sqlSb = new StringBuilder();
        sqlSb.append("INSERT INTO ").append(dbTableWrapped);
        StringBuilder keySb = new StringBuilder("(");
        StringBuilder valSb = new StringBuilder("(");
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            keySb.append("`").append(key).append("`").append(",");
            if (val == null) {//为null的忽略掉
                valSb.append(val).append(",");
            } else {
                valSb.append(StringUtils.wrap(val,"'")).append(",");
            }
        }
        keySb.deleteCharAt(keySb.lastIndexOf(","));
        valSb.deleteCharAt(valSb.lastIndexOf(","));
        keySb.append(")");
        valSb.append(")");

        sqlSb.append(" ").append(keySb).append(" VALUES ").append(valSb);
        //insert or update
        sqlSb.append(" ON DUPLICATE KEY UPDATE ");
        for (Map.Entry<String, String> entry : data.entrySet()) {
            String key = entry.getKey();
            String val = data.get(key);
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
        Map<String, String> data = rowMap.getData();
        Map<String, String> old = rowMap.getOld();
        String dbTableWrapped = wrapDT(database, table);

        StringBuilder sqlSb = new StringBuilder();
        /**必须要唯一主键，不然这种查询返回的都有值**/
        sqlSb.append("SELECT * FROM ").append(dbTableWrapped);

        /**应对无主键的情况，将旧值放入data进行查询**/
        if (old != null && !old.isEmpty()) {
            data.putAll(old);
        }
        sqlSb.append(" ").append(getWhereClause(rowMapPK, data));
        return sqlSb.toString();
    }

    /**
     * 根据pk或者旧值组装where条件子句
     * ###注意：data是将old放入data后的值，type是delete的没有old
     *
     * @param rowMapPK
     * @param data
     * @return
     */
    private String getWhereClause(RowMapPK rowMapPK, Map<String, String> data) {
        StringBuilder sb = new StringBuilder("WHERE 1 = 1");
        /**PK有可能是复合主键**/
        if (rowMapPK != null) {
            String pk = rowMapPK.getPk();
            String pkVal = rowMapPK.getPkVal();
            //先判断是否有.也就是复合主键
            if (pk.contains(".")) {
                //有顺序，等长的
                String[] pkArr = pk.split(".");
                String[] pkValArr = pkVal.split(".");
                for (int i = 0; i < pkArr.length; i++) {
                    sb.append(" AND ").append(wrapKV(pkArr[i], pkValArr[i]));
                }
            } else {
                sb.append(" AND ").append(wrapKV(pk, pkVal));
            }
        } else {
            for (Map.Entry<String, String> entry : data.entrySet()) {
                String key = entry.getKey();
                String val = entry.getValue();
                sb.append(" AND ").append(wrapKV(key, val));
            }
        }
        return sb.toString();
    }

    private String wrapKV(String key, String val) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        /**val可以为null，因为有些sql将字段的值更新为null**/
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.wrap(key, "`")).append("=");
        if (val == null) {
            sb.append(val);
        } else {
            sb.append(StringUtils.wrap(val, "'"));
        }
        return sb.toString();
    }

    private String wrapDT(String database, String table) {
        if (StringUtils.isEmpty(database) || StringUtils.isEmpty(table)) {
            return null;
        }
        return new StringBuffer().append(StringUtils.wrap(database, "`")).append(".").append(StringUtils.wrap(table, "`")).toString();
    }
}
