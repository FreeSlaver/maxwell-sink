package com.cimc.maxwell.sink.filter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cimc.maxwell.sink.filter.vo.FieldFilterDtlVo;
import com.cimc.maxwell.sink.filter.vo.FieldFilterVo;
import com.cimc.maxwell.sink.row.RowMap;

/**
 * Created by 00013708 on 2017/9/11.
 */
public class FieldFilter {

	private static final Logger log = LoggerFactory.getLogger(FieldFilter.class);
	private final Map<String, FieldFilterDtlVo> filedFiltersMap;

	public FieldFilter(final String filterConditions) {
		this.filedFiltersMap = initFieldFiltersMap(filterConditions);
	}

	private Map<String, FieldFilterDtlVo> initFieldFiltersMap(String filterConditions) {
		if (StringUtils.isEmpty(filterConditions)) {
			log.warn("filedFilters config null");
			return Collections.emptyMap();
		}
		log.info("initFiledFiltersMap,values:{}", filterConditions);
		List<FieldFilterVo> list = JSON.parseObject(filterConditions, new TypeReference<List<FieldFilterVo>>() {
		}.getType());

		if (list == null || list.isEmpty()) {
			return Collections.emptyMap();
		}
		Map<String, FieldFilterDtlVo> resultMap = new HashMap<>();
		for (FieldFilterVo fieldFilterVo : list) {
			String database = fieldFilterVo.getDatabase();
			String table = fieldFilterVo.getTable();
			String dbTable = database + "." + table;
			resultMap.put(dbTable, fieldFilterVo.getFieldCondition());
		}
		return resultMap;
	}

	public MatchCase match(RowMap rowMap) {
		if (rowMap == null) {
			return MatchCase.IGNORE;
		}
		// 如果没有过滤条件，则认为全部符合
		if (filedFiltersMap == null || filedFiltersMap.isEmpty()) {
			return MatchCase.MATCH;
		}
		String database = rowMap.getDatabase();
		String table = rowMap.getTable();
		String dbTable = database + "." + table;

		// 如果过滤条件中不包含该数据库表，则认为全部符合
		if (!filedFiltersMap.containsKey(dbTable)) {
			return MatchCase.MATCH;
		}
		
		FieldFilterDtlVo filter = filedFiltersMap.get(dbTable);
		// 如果过滤规则为空，则认为符合
		if(filter == null ) {
			return MatchCase.MATCH;
		}
		return filter.match(rowMap);
	}

}
