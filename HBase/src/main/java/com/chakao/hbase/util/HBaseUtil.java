package com.chakao.hbase.util;

import com.chakao.hbase.ORMHBaseColumn;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * bena转put,result工具类
 *
 */
public class HBaseUtil {
	public static Logger LOGGER = LoggerFactory.getLogger(HBaseUtil.class);

	/**
	 * JavaBean转换为Put
	 * 
	 * @param <T>
	 * 
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static <T> Put bean2Put(T obj) throws Exception {
		Put put = new Put(Bytes.toBytes(parseObjId(obj)));
		Class<?> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (!field.isAnnotationPresent(ORMHBaseColumn.class)) {
				continue;
			}
			field.setAccessible(true);
			ORMHBaseColumn orm = field.getAnnotation(ORMHBaseColumn.class);
			String f = orm.family();
			String q = orm.qualifier();
			if (StringUtils.isBlank(f) || StringUtils.isBlank(q)) {
				continue;
			}
			Object fieldObj = field.get(obj);
			// TODO package value
			if (fieldObj.getClass().isArray()) {
				System.out.println("show this....");
			}
			if (q.equalsIgnoreCase("rowkey") || f.equalsIgnoreCase("rowkey")) {
				continue;
			} else {
				if (field.get(obj) != null || StringUtils.isNotBlank(field.get(obj).toString())) {
					// 修改 : 增加时间版本
					long time = getTime();
					put.add(Bytes.toBytes(f), Bytes.toBytes(q), time, Bytes.toBytes(field.get(obj).toString()));
				}
			}
		}
		return put;
	}

	private static long getTime() throws ParseException {
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		String day = sf.format(new Date());
		Date today = sf.parse(day);
		long time = today.getTime();
		return time;
	}

	/**
	 * 获取Bean中的id,作为Rowkey
	 * 
	 * @param <T>
	 * 
	 * @param obj
	 * @return
	 */
	public static <T> String parseObjId(T obj) {
		Class<?> clazz = obj.getClass();
		try {
			Field field = clazz.getDeclaredField("id");
			field.setAccessible(true);
			Object object = field.get(obj);
			return object.toString();
		} catch (NoSuchFieldException e) {
			LOGGER.warn("", e);
		} catch (SecurityException e) {
			LOGGER.warn("", e);
		} catch (IllegalArgumentException e) {
			LOGGER.warn("", e);
		} catch (IllegalAccessException e) {
			LOGGER.warn("", e);
		}
		return "";
	}

	/**
	 * HBase result 转换为 bean
	 * 
	 * @param <T>
	 * @param result
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static <T> T result2Bean(Result result, T obj) throws Exception {
		if (result == null) {
			return null;
		}
		Class<?> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (!field.isAnnotationPresent(ORMHBaseColumn.class)) {
				continue;
			}
			ORMHBaseColumn orm = field.getAnnotation(ORMHBaseColumn.class);
			String f = orm.family();
			String q = orm.qualifier();
			boolean timeStamp = orm.timestamp();
			if (StringUtils.isBlank(f) || StringUtils.isBlank(q)) {
				continue;
			}
			String fieldName = field.getName();
			String value = "";
			if (f.equalsIgnoreCase("rowkey")) {
				value = new String(result.getRow());
			} else {
				value = getResultValueByType(result, f, q, timeStamp);
			}
			String firstLetter = fieldName.substring(0, 1).toUpperCase();
			String setMethodName = "set" + firstLetter + fieldName.substring(1);
			Method setMethod = clazz.getMethod(setMethodName, new Class[] { field.getType() });
			setMethod.invoke(obj, new Object[] { value });
		}
		return obj;
	}

	/**
	 * TODO add other type value
	 * 
	 * @param result
	 * @param family
	 * @param qualifier
	 * @param timeStamp
	 * @return
	 */
	private static String getResultValueByType(Result result, String family, String qualifier, boolean timeStamp) {
		if (!timeStamp) {
			return new String(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
		}
		List<Cell> cells = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		if (cells.size() == 1) {
			Cell cell = cells.get(0);
			return cell.getTimestamp() + "";
		}
		return "";
	}

}
