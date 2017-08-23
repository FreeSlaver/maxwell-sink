package com.cimc.sink;

import com.alibaba.fastjson.JSON;
import com.cimc.maxwell.sink.MySqlSinkConfig;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by 00013708 on 2017/8/16.
 */
public class MySqlSinkConfigTest {

    @Test
    public void test() throws IOException {
        //将一个properties
        Properties prop = new Properties();
        prop.load(this.getClass().getClassLoader().getResourceAsStream("maxwell-sink.properties"));
        MySqlSinkConfig config = new MySqlSinkConfig(prop);

        Map<String,?> map = config.values();

        System.out.println(JSON.toJSONString(map));
    }
}
