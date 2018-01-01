package com.cimc.maxwell.sink.filter;

public enum MatchCase {
	IGNORE, //忽略 
	MATCH, //符合条件，需要执行插入或更新
	NOMATCH //原来符合，现在不符合，需要删除原数据
}
