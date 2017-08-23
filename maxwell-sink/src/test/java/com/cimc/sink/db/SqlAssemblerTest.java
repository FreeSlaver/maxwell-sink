package com.cimc.sink.db;

import com.cimc.maxwell.sink.db.SqlAssembler;
import com.cimc.maxwell.sink.row.RowMap;
import com.cimc.maxwell.sink.row.RowMapPK;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.*;

/**
 * Created by 00013708 on 2017/8/8.
 */
public class SqlAssemblerTest {


    @org.junit.Test
    public void getSqlTest(){
        SqlAssembler assembler = new SqlAssembler();
        RowMap insertRow = insertRow();

        RowMap updateRow = updateRow();

        RowMap deleteRow = deleteRow();
        RowMapPK rowMapPK = new RowMapPK();
        rowMapPK.setPk("id");
//        rowMapPK.setPkVal("100000A01120160629150388");
        System.out.println(assembler.getSql(rowMapPK,insertRow));

//        System.out.println(assembler.getSql(updateRow));

//        System.out.println(assembler.getSql(deleteRow));
    }

    private RowMap insertRow() {
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
        return  rowMap;
    }

    private RowMap updateRow() {
        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_parcel");
        rowMap.setType("update");

        Map<String,String> data = new HashMap<>();
//        data.put("id","100000A01120160629150388");
        data.put("status","book");
        data.put("package_id","sx8888");
        data.put("pickup_mobile","1802536");
        rowMap.setData(data);

        Map<String,String> old = new HashMap<>();
        old.put("status","cancel");
        old.put("package_id","sx6666");
        old.put("pickup_mobile",null);
        rowMap.setOld(old);
        return  rowMap;
    }


    private RowMap deleteRow() {
        RowMap rowMap = new RowMap();
        rowMap.setDatabase("db_ez");
        rowMap.setTable("t_parcel");
        rowMap.setType("delete");

        Map<String,String> data = new HashMap<>();
//        data.put("id","100000A01120160629150388");
        data.put("status","book");
        data.put("package_id","sx8888");
        data.put("pickup_mobile","");
        rowMap.setData(data);

        Map<String,String> old = new HashMap<>();
        old.put("status","cancel");
        old.put("package_id","sx6666");
        old.put("pickup_mobile",null);
        rowMap.setOld(old);
        return  rowMap;

    }
    @Test
    public void getInsertSql(){
//        StringUtils.join(data.keySet(),"`,`");
//        StringUtils.join(data.values(),"','");
        Map<String,String> map = new HashMap<>();
        map.put("id","54646464121");
        map.put("box_id","sdf4s6465");
        map.put("sn","5180000A5636");
        map.put("type","small");

//        for(Map.Entry<String,String> entry:map.entrySet()){
//            String key =
//        }
        Set<String> keySet =  map.keySet();
        Collection<String> values = map.values();

        String keyStr = StringUtils.join(keySet,"`,`");
        String valStr = StringUtils.join(values,"`,`");
        System.out.println(keyStr);
        System.out.println(valStr);

        String warpped = StringUtils.wrap("hello","`");
        System.out.println(warpped);

        //对，可以这么玩，将一个的所有元素遍历，然后wrap。wrap完了之后，使用join

        /*box_id`,`id`,`sn`,`type
        sdf4s6465`,`54646464121`,`5180000A5636`,`small
        `hello`
        `box_id`,`id`,`sn`,`type`
        */

        SqlAssembler assembler = new SqlAssembler();
//        String result = assembler.wrapAndJoin(keySet,"`",",");
//        System.out.println(result);
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add(null);
        System.out.println(Arrays.toString(list.toArray()));
    }
}
