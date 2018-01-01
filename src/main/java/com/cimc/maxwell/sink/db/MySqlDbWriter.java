package com.cimc.maxwell.sink.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.cimc.maxwell.sink.MySqlSinkConfig;
import com.cimc.maxwell.sink.filter.DbTableReplacer;
import com.cimc.maxwell.sink.filter.FilterChain;
import com.cimc.maxwell.sink.filter.MatchCase;
import com.cimc.maxwell.sink.row.ExportRowMap;
import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;
import com.cimc.maxwell.sink.util.LogUtil;
import com.cimc.maxwell.sink.util.StrUtils;

/**
 * Created by 00013708 on 2017/8/9.
 */
public final class MySqlDbWriter {
	private static final Logger log = LoggerFactory.getLogger(MySqlDbWriter.class);
	private static final Logger datalog = LoggerFactory.getLogger("datalog");

	private CachedConnectionHolder connectionHolder;

	private final SqlAssembler assembler;
	private DbTableReplacer dbTableReplacer;
	private final FilterChain filterChain;

	public MySqlDbWriter(MySqlSinkConfig config) {
		this.connectionHolder = new CachedConnectionHolder(config.mysqlUsername, config.mysqlPassword, config.mysqlUrl,
				config.mysqlDriver);

		this.assembler = new SqlAssembler();
		this.dbTableReplacer = new DbTableReplacer(config.dbtables);
		this.filterChain = new FilterChain(config);
	}

	public void batchWrite(final Collection<SinkRecord> records) throws SQLException {
		if (records == null || records.isEmpty()) {
			return;
		}
		final List<String> sqlList = new ArrayList<>(records.size());
		for (SinkRecord record : records) {
			String topic = record.topic();
			/** 没有pk的key是多少？ **/
			String key = StrUtils.valueOf(record.key());
			String val = StrUtils.valueOf(record.value());
			log.info("===>>>topic:{},partition:{},offset:{},\n===>>>key:{},value:{}", topic, record.kafkaPartition(),
					record.kafkaOffset(), record.key(), record.value());

			RowMapPK rowMapPK = getRowMapPK(key);
			RowMap rowMap = JSON.parseObject(val, RowMap.class);
			MatchCase matchCase = filterChain.matchCase(rowMap);
			ExportRowMap exportRowMap = new ExportRowMap(rowMapPK, rowMap);
			datalog.info(exportRowMap.toString());
			LogUtil.backUp(exportRowMap);


				rowMap = dbTableReplacer.replace(rowMap);
				log.info("===>>>Assembler RowMap:{}", rowMap.toString());
				String sql = assembler.getSql(matchCase, rowMapPK, rowMap);
				log.info("===>>>Assembler GET SQL:{}", sql);
				if (StringUtils.isNotEmpty(sql)) {
					sqlList.add(sql);
				}
		}

		flush(sqlList);
	}

	private RowMapPK getRowMapPK(String recordKey) {
		if (StringUtils.isEmpty(recordKey)) {
			return null;
		}
		Map<String, Object> rowMapKey = JSON.parseObject(recordKey);
		rowMapKey.remove("database");
		rowMapKey.remove("table");

		// 剩下唯一的元素
		for (Map.Entry<String, Object> entry : rowMapKey.entrySet()) {
			String key = entry.getKey();
			if (!key.contains("pk.")) {
				return null;
			}
			String val = StrUtils.valueOf(entry.getValue());
			String pk = key.replace("pk.", "");
			String pkVal = val;
			return new RowMapPK(pk, pkVal);
		}
		return null;
	}

	public void flush(final List<String> sqlBatch) throws SQLException {
		if (sqlBatch == null || sqlBatch.isEmpty()) {
			return;
		}
		Connection connection = connectionHolder.getValidConnection();
		connection.setAutoCommit(false);

		Statement statement = connection.createStatement();
		for (String sql : sqlBatch) {
			statement.addBatch(sql);
		}
		int[] updateCountArr = statement.executeBatch();
		if (updateCountArr.length != sqlBatch.size()) {
			throw new ConnectException(String.format("updateCountArr size:(%d) not equals to sqlBatch size:(%d)",
					updateCountArr.length, sqlBatch.size()));
		}
		connection.commit();
		statement.close();
	}

	public synchronized void closeQuietly() {
		this.connectionHolder.closeQuietly();
	}

	public int write(String sql) throws SQLException {
		if (StringUtils.isEmpty(sql)) {
			return 0;
		}
		Connection connection = connectionHolder.getValidConnection();
		Statement statement = connection.createStatement();
		log.info("===>Execute SQL:{}", sql);
		int updateCount = statement.executeUpdate(sql);
		statement.close();
		return updateCount;
	}

}
