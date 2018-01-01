package com.cimc.maxwell.sink.filter;

import com.cimc.maxwell.sink.MySqlSinkConfig;
import com.cimc.maxwell.sink.row.RowMap;

/**
 * Created by 00013708 on 2017/8/30.
 */
public class FilterChain {

	private final DMLFilter dmlFilter;
	private final FieldFilter fieldFilter;

	public FilterChain(MySqlSinkConfig config) {
		this.fieldFilter = new FieldFilter(config.fieldFilters);
		this.dmlFilter = new DMLFilter(config.dmlFilters);
	}

	/**
	 * <pre>
	 * 场景
	 * 1、不符合DML操作，忽略
	 * 2、插入操作，符合字段过滤，执行插入
	 * 3、插入操作，不符合字段过滤，忽略
	 * 4、更新操作，旧数据符合字段过滤，新数据不符合字段过滤，执行删除
	 * 5、更新操作，旧数据不符合字段过滤，新数据符合字段过滤，执行插入
	 * 6、更新操作，旧数据不符合字段过滤，新数据不符合字段过滤，忽略
	 * 7、更新操作，旧数据符合字段过滤，新数据符合字段过滤，如果忽略更新，则忽略，否则更新
	 * 8、删除操作，旧数据符合过滤条件，执行删除
	 * 9、删除操作，旧数据不符合过滤条件，忽略
	 * 
	 * 返回：
	 * 0 - 表示忽略
	 * 1 - 表示插入/更新
	 * 2 - 表示删除
	 * </pre>
	 */

	public MatchCase matchCase(RowMap rowMap) {
		if (!dmlFilter.match(rowMap)) {
			return MatchCase.IGNORE;
		}
		return fieldFilter.match(rowMap);
	}
}
