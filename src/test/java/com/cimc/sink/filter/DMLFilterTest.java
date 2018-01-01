package com.cimc.sink.filter;

import com.cimc.maxwell.sink.filter.DMLFilter;
import com.cimc.maxwell.sink.row.RowMap;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by 00013708 on 2017/8/30.
 */
public class DMLFilterTest {

    @Test
    public void matchTest(){
        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_term");
        rowMap.setType("insert");

        String dmlFilterConfig = null;
        DMLFilter dmlFilter = new DMLFilter(dmlFilterConfig);
        boolean result = dmlFilter.match(rowMap);
        Assert.assertTrue(result);


        dmlFilterConfig = "";
        dmlFilter = new DMLFilter(dmlFilterConfig);
        result = dmlFilter.match(rowMap);
        Assert.assertTrue(result);


        dmlFilterConfig = "{\"db_ez.t_term\" : {\"dml_type\": \"insert,update\"}}";
        dmlFilter = new DMLFilter(dmlFilterConfig);
        result = dmlFilter.match(rowMap);
        Assert.assertTrue(result);

        rowMap.setType("delete");
        result = dmlFilter.match(rowMap);
        Assert.assertFalse(result);

    }
}
