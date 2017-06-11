package com.chakao.hbase.service.impl;

import com.chakao.hbase.ORMHBaseTable;
import com.chakao.hbase.service.HBaseConnectionService;
import com.chakao.hbase.util.HBaseUtil;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class HBaseConnectionServiceImpl implements HBaseConnectionService, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Configuration config;
	private Logger logger = LoggerFactory.getLogger(HBaseConnectionServiceImpl.class);

	/**
	 * 
	 */
	public HBaseConnectionServiceImpl() {
	}

	public void addColumnFamilys(String tableName, String... columnFamilys) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(getConfig());
			if (!admin.tableExists(tableName)) {
				return;
			}
			for (String family : columnFamilys) {
				admin.addColumn(tableName, new HColumnDescriptor(Bytes.toBytes(family)));
			}
			admin.flush(tableName);
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}

	}

	public void createTable(String tableName, String... columnFamilys) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(getConfig());
			if (admin.tableExists(tableName)) {
				logger.warn("HBase中已经存在该命名的表,请修改名称或删除该表后重建");
				return;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for (String columnFamily : columnFamilys) {
				tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(tableDescriptor);
		} catch (Exception e) {
			logger.warn("创建HBase表失败;", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	private void delete(List<Delete> deletes, String tableName) {
		HConnection connection = null;
		HTableInterface table = null;
		try {
			if (StringUtils.isBlank(tableName)) {
				return;
			}
			connection = getHbaseConnection();
			table = connection.getTable(tableName);
			table.delete(deletes);
		} catch (IOException e) {
			logger.warn("执行删除失败;", e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	public HConnection getHbaseConnection() throws IOException {
		return HConnectionManager.createConnection(getConfig());
	}

	public void delete(String tableName, String... rowkeys) {
		List<Delete> deletes = new ArrayList<Delete>();
		for (String rowkey : rowkeys) {
			if (StringUtils.isBlank(rowkey)) {
				continue;
			}
			deletes.add(new Delete(Bytes.toBytes(rowkey)));
		}
		delete(deletes, tableName);
	}

	public <T> void delete(T obj, String... rowkeys) {
		String tableName = "";
		tableName = getORMTable(obj);
		if (StringUtils.isBlank(tableName)) {
			return;
		}
		List<Delete> deletes = new ArrayList<Delete>();
		for (String rowkey : rowkeys) {
			if (StringUtils.isBlank(rowkey)) {
				continue;
			}
			deletes.add(new Delete(Bytes.toBytes(rowkey)));
		}
		delete(deletes, tableName);
	}

	public void deleteColumnFamilys(String tableName, String... columnFamilys) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(getConfig());
			if (!admin.tableExists(tableName)) {
				return;
			}
			for (String family : columnFamilys) {
				admin.deleteColumn(tableName, family);
			}
			admin.flush(tableName);
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	public void deleteTable(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(getConfig());
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	public List<String> familys(String tableName) {
		HConnection connection = null;
		HTableInterface table = null;
		try {
			List<String> columns = new ArrayList<String>();
			connection = getHbaseConnection();
			table = connection.getTable(tableName);
			if (table == null) {
				return columns;
			}
			HTableDescriptor tableDescriptor = table.getTableDescriptor();
			HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
			for (HColumnDescriptor columnDescriptor : columnDescriptors) {
				String columnName = columnDescriptor.getNameAsString();
				columns.add(columnName);
			}
			return columns;
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
		return new ArrayList<String>();
	}

	/**
	 * obj : input bean rowkeys : a cup of rowkeys reutrn : out bean same with
	 * input
	 */
	public <T> List<T> get(T obj, String... rowkeys) {
		List<T> objs = new ArrayList<T>();
		String tableName = getORMTable(obj);
		if (StringUtils.isBlank(tableName)) {
			return objs;
		}
		List<Result> results = getResults(tableName, rowkeys);
		if (results.isEmpty()) {
			return objs;
		}
		for (Result result : results) {
			if (result == null || result.isEmpty()) {
				continue;
			}
			T bean = null;
			try {
				bean = HBaseUtil.result2Bean(result, obj);
			} catch (Exception e) {
				logger.warn("", e);
			}
			objs.add(bean);
		}
		return objs;
	}

	/**
	 * 获取配置，当前使用默认配置
	 * 
	 * @return the config
	 */
	public Configuration getConfig() {
		if (config == null) {
			config = HBaseConfiguration.create();
		}
		return config;
	}

	private String getORMTable(Object obj) {
		ORMHBaseTable table = obj.getClass().getAnnotation(ORMHBaseTable.class);
		return table.tableName();
	}

	private List<Result> getResults(String tableName, String... rowkeys) {
		List<Result> resultList = new ArrayList<Result>();
		List<Get> gets = new ArrayList<Get>();
		for (String rowkey : rowkeys) {
			if (StringUtils.isBlank(rowkey)) {
				continue;
			}
			Get get = new Get(Bytes.toBytes(rowkey));
			gets.add(get);
		}
		HConnection connection = null;
		HTableInterface table = null;
		try {
			connection = getHbaseConnection();
			table = connection.getTable(tableName);
			Result[] results = table.get(gets);
			Collections.addAll(resultList, results);
			return resultList;
		} catch (IOException e) {
			logger.warn("", e);
			return resultList;
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}

	}

	public <T> void insert(String tableName, T... objs) {
		List<Put> puts = new ArrayList<Put>();
		for (Object obj : objs) {
			if (obj == null) {
				continue;
			}
			try {
				Put put = HBaseUtil.bean2Put(obj);
				puts.add(put);
			} catch (Exception e) {
				logger.warn("", e);
			}
		}
		savePut(puts, tableName);
	}

	public <T> void insert(T... objs) {
		List<Put> puts = new ArrayList<Put>();
		String tableName = "";
		for (Object obj : objs) {
			if (obj == null) {
				continue;
			}
			tableName = getORMTable(obj);
			try {
				Put put = HBaseUtil.bean2Put(obj);
				puts.add(put);
			} catch (Exception e) {
				logger.warn("", e);
			}
		}
		savePut(puts, tableName);
	}

	private void savePut(List<Put> puts, String tableName) {
		HConnection connection = null;
		HTableInterface table = null;
		try {
			if (StringUtils.isBlank(tableName)) {
				return;
			}
			connection = getHbaseConnection();
			table = connection.getTable(tableName);
			table.put(puts);
		} catch (IOException e) {
			logger.warn("存储失败;", e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	public List<String> tables() {
		HConnection connection = null;
		try {
			connection = getHbaseConnection();
			TableName[] tableNames = connection.listTableNames();
			List<String> tables = new ArrayList<String>();
			for (TableName tableName : tableNames) {
				String name = tableName.getNameAsString();
				tables.add(name);
			}
			return tables;
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
		return new ArrayList<String>();
	}
}
