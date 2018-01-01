package com.cimc.sink.util;

import com.cimc.maxwell.sink.row.ExportRowMap;
import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;
import com.cimc.maxwell.sink.util.LogUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 00013708 on 2017/9/13.
 */
public class LogUtilTest {

    @Test
    public void backupTest() {
        RowMapPK rowMapPK = new RowMapPK("sn", "518000");

        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_term");
        rowMap.setType("insert");

        Map<String, Object> data = new HashMap<>();
        data.put("id", "100000A01120160629150388");
        data.put("sn", "518000");
        data.put("box_id", "");
        data.put("status", "book");
        data.put("package_id", "sx8888");
        data.put("pickup_mobile", null);
        data.put("take_code", "456685");
        data.put("retrieve_code", "333333");
        rowMap.setData(data);

        ExportRowMap exportRowMap = new ExportRowMap(rowMapPK, rowMap);

        LogUtil.backUp(exportRowMap);

    }
}
