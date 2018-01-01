package com.cimc.maxwell.sink.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cimc.maxwell.sink.row.ExportRowMap;

/**
 * Created by 00013708 on 2017/9/13.
 */
public class LogUtil {
	private static final Logger log = LoggerFactory.getLogger(LogUtil.class);
	// private static final Logger datalog = LoggerFactory.getLogger("datalog");

	private static final String TERMINAL_BACKUP_PATH = "/data/kafka-connect/logs/backup/";

	public static void backUp(ExportRowMap exportRowMap) {
		// 按照终端过滤，不同的终端写不同的文件
		Map<String, Object> data = exportRowMap.getData();
		String sn = StrUtils.valueOf(data.get("sn"));
		if (StringUtils.isEmpty(sn)) {
			return;
		}
		File termBackupFile = new File(TERMINAL_BACKUP_PATH + sn);
		String timeStr = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:sss").format(new Date());
		String rowMapStr = exportRowMap.toString();
		String result = new StringBuilder().append(timeStr).append(" ").append(rowMapStr).append("\n").toString();
		try {
			FileUtils.write(termBackupFile, result, Charset.defaultCharset(), true);
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			log.info(result);
		}
	}
}
