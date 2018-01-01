package com.cimc.maxwell.sink.filter;

import com.cimc.maxwell.sink.row.RowMap;

/**
 * Created by 00013708 on 2017/8/30.
 */
public interface Filter {

	boolean match(RowMap rowMap);
}
