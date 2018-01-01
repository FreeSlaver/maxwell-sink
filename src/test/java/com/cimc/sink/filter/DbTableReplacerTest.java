package com.cimc.sink.filter;

import com.cimc.maxwell.sink.filter.DbTableReplacer;
import com.cimc.maxwell.sink.row.RowMap;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * Created by 00013708 on 2017/8/22.
 */
public class DbTableReplacerTest {

    @Test
    public void redirectDbTest() {
//        String topicTargetDb = "{\"db_ez.t_parcel\":\"test.t_parcel\",\"db_ez.t_box\":\"test.t_box\"}";
        String topicTargetDb = "{\"db_ez.t_term\":\"db_ez.t_term\"}";


        DbTableReplacer redirector = new DbTableReplacer(topicTargetDb);

        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_parcel");
        rowMap.setType("insert");

        Map<String, Object> data = new HashMap<>();
        data.put("id", "100000A01120160629150388");
        data.put("sn", "100000A011");
        data.put("box_id", "");
        data.put("status", "book");
        data.put("package_id", "sx8888");
        data.put("pickup_mobile", null);
        data.put("take_code", "456685");
        data.put("retrieve_code", "333333");
        rowMap.setData(data);


        RowMap redirectRowMap = redirector.replace(rowMap);

        String targetDb = redirectRowMap.getDatabase();
        Assert.assertTrue(targetDb.equals("test"));

    }

    @Test
    public void table2Topic() {
        String dbEzTables = "t_book_parcel,t_book_queue,t_box_type,t_cabinet,t_cabinet_type,t_config,t_config_term_relocate,t_external_device," +
                "t_lock_box,t_opt_term,t_prop_config,t_server_config,t_server_route,t_server_whitelist,t_term";

        String dbEzLogTables = "t_ez_changebox,t_ez_login_code,t_ez_remark,t_ez_term_warn_log";


        String[] tableArr = dbEzTables.split(",");
        List<String> dbEz = new ArrayList<>();
        for (String table : tableArr) {
            table = "estation.db_ez." + table;
            dbEz.add(table);
        }
        //含有log的转换成db_ez_log
        String[] logTableArr = dbEzLogTables.split(",");
        List<String> dbEzLog = new ArrayList<>();
        for (String table : logTableArr) {
            table = "estation.db_ez_log." + table;
            dbEzLog.add(table);
        }

        System.out.println(StringUtils.join(dbEz, ","));
        System.out.println(StringUtils.join(dbEzLog, ","));
    }
}
