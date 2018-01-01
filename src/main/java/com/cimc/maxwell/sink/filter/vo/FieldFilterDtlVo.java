package com.cimc.maxwell.sink.filter.vo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.cimc.maxwell.sink.filter.MatchCase;
import com.cimc.maxwell.sink.row.RowMap;

public class FieldFilterDtlVo {

	private String field;

	private List<String> fitVals;

	private List<String> regex;

	private Boolean ignoreUpdate;

	private List<Pattern> patterns;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public List<String> getFitVals() {
		return fitVals;
	}

	public void setFitVals(List<String> fitVals) {
		this.fitVals = fitVals;
	}

	public List<String> getRegex() {
		return regex;
	}

	public void setRegex(List<String> regex) {
		this.regex = regex;
		patterns = new ArrayList<>();
		for (String r : regex) {
			patterns.add(Pattern.compile(r));
		}
	}

	public List<Pattern> getPatterns() {
		return patterns;
	}

	public void setPatterns(List<Pattern> patterns) {
		this.patterns = patterns;
	}

	public Boolean getIgnoreUpdate() {
		return ignoreUpdate;
	}

	public void setIgnoreUpdate(Boolean ignoreUpdate) {
		this.ignoreUpdate = ignoreUpdate;
	}

	private boolean match(String s) {
		if (StringUtils.isEmpty(s)) {
			return false;
		}
		if (fitVals.contains(s)) {
			return true;
		}
		for (Pattern p : patterns) {
			if (p.matcher(s).matches()) {
				return true;
			}
		}
		return false;
	}

	public MatchCase match(RowMap rowMap) {
		// 如果该行数据为空，则直接忽略
		if (rowMap == null) {
			return MatchCase.IGNORE;
		}
		Object oo = null;
		if (rowMap.getOld() != null) {
			oo = rowMap.getOld().get(field);
		}
		String oldVal = oo == null ? null : oo.toString();
		boolean oldMatch = match(oldVal);
		Object o = rowMap.getData().get(field);
		String newVal = o == null ? null : o.toString();
		boolean newMatch = match(newVal);
		if (rowMap.getType().equalsIgnoreCase("INSERT")) {
			if (newMatch) {
				return MatchCase.MATCH;
			} else {
				return MatchCase.IGNORE;
			}
		}
		if (rowMap.getType().equalsIgnoreCase("UPDATE")) {
			if (!oldMatch && !newMatch) {
				return MatchCase.IGNORE;
			}
			if (oldMatch && !newMatch) {
				return MatchCase.NOMATCH;
			}
			if (oldMatch && newMatch) {
				return MatchCase.MATCH;
			}
			if (!oldMatch && newMatch) {
				return MatchCase.MATCH;
			}
		}
		if (rowMap.getType().equalsIgnoreCase("DELETE")) {
			if (match(oldVal)) {
				return MatchCase.NOMATCH;
			}
		}
		return MatchCase.IGNORE;
	}

}
