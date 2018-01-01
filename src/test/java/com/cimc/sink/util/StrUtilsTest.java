package com.cimc.sink.util;

import com.cimc.maxwell.sink.util.StrUtils;
import org.junit.Test;

/**
 * Created by 00013708 on 2017/8/29.
 */
public class StrUtilsTest {

    @Test
    public void escapeSqlTest(){
        String val = "451028974'23";
        String result = StrUtils.escapeSql(val);
        System.out.println(result);


    }


    @Test
    public void valOfTest(){
        String val = "451028974'23";
        String result = StrUtils.valueOf(val);
        System.out.println(result);
    }
}
