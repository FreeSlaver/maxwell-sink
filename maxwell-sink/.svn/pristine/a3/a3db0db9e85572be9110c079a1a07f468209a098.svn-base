package com.cimc.maxwell.sink.filter;

import com.alibaba.fastjson.JSON;
import com.cimc.maxwell.sink.row.RowMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 00013708 on 2017/8/16.
 * 根据topic设置的目标database，将rowMap中的database进行重定向
 */
public class DatabaseRedirector {
    private static final Logger log = LoggerFactory.getLogger(DatabaseRedirector.class);
    private final Map<String, String> topicDbMap;

    public DatabaseRedirector(final String topicTargetDB) {
        this.topicDbMap = initTopicDbMap(topicTargetDB);
    }

    /**
     * 消费的主题和导入的db对应关系
     *
     * @param topicTargetDB example:estation.db_ez.t_parcel:db_ez,estation.db_ez.t_box:db_ez
     * @return example: {"estation.db_ez.t_parcel" : "db_ez","estation.db_ez.t_box" : "db_ez"}
     */
    private Map<String, String> initTopicDbMap(String topicTargetDB) {
        if (StringUtils.isEmpty(topicTargetDB)) {
            log.warn("topicTargetDb config null");
            throw new IllegalArgumentException("topicTargetDb config null");
        }
        final Map<String, String> map = new HashMap<>();
        String[] topicDbPairs = topicTargetDB.split(",");
        for (String pair : topicDbPairs) {//有重复代码，可以统一处理
            String[] arr = pair.split(":");
            String topic = arr[0];
            String db = arr[1];
            map.put(topic, db);
        }
        log.info("===>>>initTopicDbMap success.values:{}", JSON.toJSONString(map));
        return map;
    }

    public RowMap redirectDb(final String topic, final RowMap rowMap) {
        if (StringUtils.isEmpty(topic) || rowMap == null) {
            return null;
        }
        //判断是否要根据topic替换目标数据库
        if (topicDbMap.containsKey(topic)) {
            String targetDb = topicDbMap.get(topic);
            log.info("===>>>topic:{},set database: {} to {}", topic, rowMap.getDatabase(), targetDb);
            rowMap.setDatabase(targetDb);
        }
        return rowMap;
    }
}
