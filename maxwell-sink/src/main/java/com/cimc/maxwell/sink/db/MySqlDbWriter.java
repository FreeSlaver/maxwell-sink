package com.cimc.maxwell.sink.db;

import com.alibaba.fastjson.JSON;
import com.cimc.maxwell.sink.MySqlSinkConfig;
import com.cimc.maxwell.sink.filter.ConditionFilter;
import com.cimc.maxwell.sink.filter.DatabaseRedirector;
import com.cimc.maxwell.sink.row.ExportRowMap;
import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by 00013708 on 2017/8/9.
 */
public final class MySqlDbWriter {
    private static final Logger log = LoggerFactory.getLogger(MySqlDbWriter.class);
    private static final Logger datalog = LoggerFactory.getLogger("datalog");

    private CachedConnectionHolder connectionHolder;


    private final SqlAssembler assembler;
    private DatabaseRedirector dbRedirector;
    private final ConditionFilter filter;


    public MySqlDbWriter(MySqlSinkConfig config) {
        this.connectionHolder = new CachedConnectionHolder(config.mysqlUsername, config.mysqlPassword, config.mysqlUrl, config.mysqlDriver);

        this.assembler = new SqlAssembler();
        this.dbRedirector = new DatabaseRedirector(config.topicTargetDB);
        this.filter = new ConditionFilter(config.filterConditions);
    }

    public void batchWrite(final Collection<SinkRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return;
        }
        List<String> sqlList = new ArrayList<>();
        for (SinkRecord record : records) {
            String topic = record.topic();
            /**没有pk的key是多少？**/
            String key = (String) record.key();
            String val = (String) record.value();
            log.info("===>>>topic:{},partition:{},offset:{},\n===>>>key:{},value:{}", topic, record.kafkaPartition(), record.kafkaOffset(), record.key(), record.value());

            RowMapPK rowMapPK = getRowMapPK(key);
            RowMap rowMap = JSON.parseObject(val, RowMap.class);

            /**数据过滤**/
            if (filter.match(rowMap)) {
                //将新老数据输出到指定文件
                ExportRowMap exportRowMap = new ExportRowMap(rowMapPK, rowMap);
                datalog.info(exportRowMap.toString());

                rowMap = dbRedirector.redirectDb(topic, rowMap);
                log.info("===>>>Assembler RowMap:{}", rowMap.toString());
                String sql = assembler.getSql(rowMapPK, rowMap);
                log.info("===>>>Assembler GET SQL:{}", sql);
                if (StringUtils.isNotEmpty(sql)) {
                    sqlList.add(sql);
                }
            }
        }

        flush(sqlList);
    }

    public void flush(final List<String> sqlBatch) throws SQLException {
        if (sqlBatch == null || sqlBatch.isEmpty()) {
            return;
        }
        Connection connection = connectionHolder.getValidConnection();
        connection.setAutoCommit(false);

        Statement statement = connection.createStatement();
        for (String sql : sqlBatch) {
            log.info("===>>>statement addBatch sql:{}", sql);
            statement.addBatch(sql);
        }
        int[] updateCountArr = statement.executeBatch();
        if (updateCountArr.length != sqlBatch.size()) {
            throw new ConnectException(String.format("updateCountArr size:(%d) not equals to sqlBatch size:(%d)", updateCountArr.length, sqlBatch.size()));
        }
        connection.commit();
        statement.close();
    }

    private RowMapPK getRowMapPK(String recordKey) {
        if (StringUtils.isEmpty(recordKey)) {
            return null;
        }
        Map<String, Object> rowMapKey = JSON.parseObject(recordKey);

        String pkColumns = null;
        String pkVal = null;
        for (Map.Entry<String, Object> entry : rowMapKey.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("pk.")) {
                //得到所有的PK字段，可能是联合主键
                pkColumns = key.replace("pk.", "");
                pkVal = (String) entry.getValue();
            }
            break;
        }
        if (StringUtils.isNotEmpty(pkColumns) && StringUtils.isNotEmpty(pkVal)) {
            RowMapPK rowMapPK = new RowMapPK(pkColumns, pkVal);
            log.info("===>getRowMapPK:{}", rowMapPK.toString());
            return rowMapPK;
        }
        return null;
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
