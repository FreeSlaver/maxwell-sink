package com.cimc.sink.filter;

import com.cimc.maxwell.sink.filter.DatabaseRedirector;
import com.cimc.maxwell.sink.row.RowMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 00013708 on 2017/8/22.
 */
public class DatabaseRedirectorTest {

    @Test
    public void redirectDbTest(){
        String topicTargetDb = "estation.db_ez.t_parcel:test,estation.db_ez.t_box:test";

        DatabaseRedirector redirector = new DatabaseRedirector(topicTargetDb);

        String topic = "estation.db_ez.t_parcel";
        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_parcel");
        rowMap.setType("insert");

        Map<String,String> data = new HashMap<>();
        data.put("id","100000A01120160629150388");
        data.put("sn","100000A011");
        data.put("box_id","");
        data.put("status","book");
        data.put("package_id","sx8888");
        data.put("pickup_mobile",null);
        data.put("take_code","456685");
        data.put("retrieve_code","333333");
        rowMap.setData(data);


        RowMap redirectRowMap = redirector.redirectDb(topic,rowMap);

        String targetDb = redirectRowMap.getDatabase();
        Assert.assertTrue(targetDb.equals("test"));

    }
}
