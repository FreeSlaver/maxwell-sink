package com.cimc.maxwell.sink.filter;

import com.alibaba.fastjson.JSON;
import com.cimc.maxwell.sink.row.RowMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by 00013708 on 2017/8/11.
 */
public class ConditionFilter {
    private static final Logger log = LoggerFactory.getLogger(ConditionFilter.class);
    private final Map<String, Set<String>> conditionsMap;

    public ConditionFilter(final String filterConditions) {
        this.conditionsMap = initConditionsMap(filterConditions);
    }

    private Map<String, Set<String>> initConditionsMap(String filterConditions) {
        if (StringUtils.isEmpty(filterConditions)) {
            log.warn("filterConditions config null");
            return Collections.emptyMap();
        }
        Map<String, Set<String>> map = new HashMap<>();
        String[] conditionPairs = filterConditions.split(";");
        for (String condition : conditionPairs) {
            String[] arr = condition.split(":");
            String key = arr[0];
            String val = arr[1];
            String[] valArr = val.split(",");
            Set<String> set = new HashSet<String>(Arrays.asList(valArr));
            map.put(key, set);
        }
        log.info("initConditionsMap success,values:{}", JSON.toJSONString(map));
        return map;
    }

    public boolean match(RowMap rowMap) {
        if (rowMap == null) {
            return false;
        }
        if (conditionsMap == null || conditionsMap.isEmpty()) {
            return true;
        }
        Map<String, String> data = rowMap.getData();
        if (data == null || data.isEmpty()) {
            return false;
        }
        String type = rowMap.getType();
        //遍历条件，所有满足返回真
        boolean fitAllConditions = true;
        for (Map.Entry<String, Set<String>> entry : conditionsMap.entrySet()) {
            String conditionKey = entry.getKey();
            Set<String> conditionVal = entry.getValue();
            //得到data中的实际值
            String dataVal = data.get(conditionKey);

            if (type.equalsIgnoreCase("update")) {
                //先判断dataVal是否符合，不符合判断oldVal是否符合，都不符合，返回false；
                if (!conditionVal.contains(dataVal)) {
                    Map<String, String> old = rowMap.getOld();
                    //得到old中的原始值（针对特定的如柜子解绑操作）
                    String oldVal = old.get(conditionKey);
                    if (StringUtils.isEmpty(oldVal) || !conditionVal.contains(oldVal)) {
                        fitAllConditions = false;
                        break;
                    }
                }
            } else {
                if (!conditionVal.contains(dataVal)) {
                    fitAllConditions = false;
                    break;
                }
            }
        }
        return fitAllConditions;
    }

}
