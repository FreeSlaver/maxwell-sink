package com.cimc.sink.filter;

import com.cimc.maxwell.sink.filter.ConditionFilter;
import com.cimc.maxwell.sink.row.RowMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 00013708 on 2017/8/22.
 */
public class ConditionFilterTest {

    @Test
    public void matchTest() {
        ConditionFilter filter1 = new ConditionFilter(null);
        boolean matchResult = filter1.match(new RowMap());
        Assert.assertTrue(matchResult);

        String filterConditions = "sn:100000A011,100000A012,100000A013;";
        ConditionFilter filter = new ConditionFilter(filterConditions);

        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_parcel");
        rowMap.setType("insert");

        Map<String, String> data = new HashMap<>();
        data.put("id", "100000A01120160629150388");
        data.put("sn", "100000A011");
        data.put("box_id", "");
        data.put("status", "book");
        data.put("package_id", "sx8888");
        data.put("pickup_mobile", null);
        data.put("take_code", "456685");
        data.put("retrieve_code", "333333");
        rowMap.setData(data);

        boolean matched2 = filter.match(rowMap);
        Assert.assertTrue(matched2);

        //测试不符合
        data.put("sn", "xxxxx");
        rowMap.setData(data);
        boolean matched3 = filter.match(rowMap);
        Assert.assertFalse(matched3);

        //测试update
        rowMap.setType("update");
        data.remove("sn");
        rowMap.setData(data);
        Map<String,String> old = new HashMap<>();
        old.put("sn", "100000A011");
        rowMap.setOld(old);

        boolean matched4 = filter.match(rowMap);
        Assert.assertTrue(matched4);
    }
}
