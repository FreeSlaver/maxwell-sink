package com.cimc.sink;

import com.cimc.maxwell.sink.MySqlSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by 00013708 on 2017/8/16.
 */
public class MySqlSinkTaskTest {

    @Test
    public void putTest() throws IOException {
        MySqlSinkTask task = startTask();
        final Collection<SinkRecord> records = new ArrayList<>();

        String topic = "estation.db_ez.t_box";
        int partition = 0;
        long kafkaOffset = 100;

        String key = "{\"database\":\"db_ez\",\"table\":\"t_parcel\",\"pk.id\":\"100000A01120160629150390\"}";
        String val = "{\"commit\":true,\"data\":{\"book_code\":\"0\",\"book_expire_time\":\"2016-06-01 11:55:39\",\"box_id\":\"EZ004016145026\",\"box_type\":\"1\",\"business_type\":\"1\",\"channel_id\":\"0\",\"code_expire_time\":\"2016-07-22 10:19:40\",\"company_id\":\"433\",\"create_time\":\"2016-08-10 09:26:14\",\"delivery_time\":\"2016-06-29 15:04:17\",\"expire_time\":\"2016-07-01 15:04:17\",\"id\":\"100000A01120160629150390\",\"is_old\":\"2\",\"package_id\":\"sxFuckFuck\",\"partner_cid\":\"0\",\"postman_mobile\":\"18618307356\",\"postman_name\":\"乔德康\",\"postman_uid\":\"5357\",\"reminder_num\":\"15\",\"retrieve_code\":\"000000\",\"sn\":\"100000A013\",\"status\":\"exceptional\",\"take_code\":\"281587\",\"take_mobile\":\"13801174051\"},\"database\":\"test\",\"old\":{\"package_id\":\"sxDSDSDS\"},\"table\":\"t_parcel\",\"ts\":1502884124,\"type\":\"update\",\"xid\":11524421}";

        SinkRecord record = new SinkRecord(topic, partition, null, key, null, val, kafkaOffset);

        records.add(record);

        task.put(records);
    }


    @Test
    public void startTest() throws IOException {
        startTask();
    }

    private MySqlSinkTask startTask() throws IOException {
        MySqlSinkTask task = new MySqlSinkTask();
        final Properties prop = new Properties();
        prop.load(this.getClass().getClassLoader().getResourceAsStream("maxwell-sink.properties"));

        Map<String, String> map = new HashMap<>();
        for (Map.Entry<Object, Object> entry : prop.entrySet()) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            map.put(key, val);
        }

        task.start(map);
        return task;
    }


}
