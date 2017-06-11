package somethingUtils.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class HBaseHelper implements Serializable{

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory
			.getLogger(HBaseHelper.class);

	private final Map<String, WriteTableThreadWrapper> threadMap = new HashMap<String, WriteTableThreadWrapper>();
	private String hbaseRootDir = "";
	private String hbaseZookeeperQuorum = "";
	private int writeBuf = 2; // unit M
	private int commitPeriod = 5; // unit is second;
	private static Configuration configure = null;
	private int catchSize = 500;

	private int queueSize = 150000;
	private int commitSize = 10000;
	private int hbaseCommitThreadNumber = 3;
	private int hbaseBatchCommitNumber = 1000;
	private AtomicInteger failedCounter = new AtomicInteger();

	// hbase创建configure
	private Configuration getConfiguration() {
		if (configure != null) {
			return configure;
		}

		synchronized (this) {
			if (configure != null) {
				return configure;
			}

			configure = HBaseConfiguration.create();
			configure.addResource("hbase-site.xml");
		}

		return configure;
	}

	/**
	 * @Title: create
	 * @Description: TODO(创建一张表 )
	 * @param @param tableName 表名
	 * @param @param columnFamily 列族
	 * @param @throws IOException 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public void create(String tableName, String[] columnFamily)
			throws IOException {
		Connection connection = null;
		Admin admin = null;
		try {

			connection = ConnectionFactory.createConnection(getConfiguration());
			admin = connection.getAdmin();
			HTableDescriptor desc = new HTableDescriptor(
					TableName.valueOf(tableName));
			for (int i = 0; i < columnFamily.length; i++) {
				desc.addFamily(new HColumnDescriptor(columnFamily[i]));
			}
			if (admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println("表已经存在");

			} else {
				admin.createTable(desc);
				System.out.println("create table Success!");
			}
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}

	/**
	 * @Title: put
	 * @Description: TODO( 添加一条记录 )
	 * @param @param tableName
	 * @param @param row rowkey
	 * @param @param columnFamily 列族
	 * @param @param column 列
	 * @param @param data 数据
	 * @param @throws Exception 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public void put(String tableName, String row, String columnFamily,
			String column, String data) throws Exception {
		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		Put p1 = new Put(Bytes.toBytes(row));
		p1.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(data));
		table.put(p1);

		table.close();
		System.out.println("put ok!!");
	}

	/**
	 * @Title: put
	 * @Description: TODO(列族下多列插入数据)
	 * @param @param tableName 表名
	 * @param @param row rowkey
	 * @param @param values 列数据
	 * @param @throws Exception 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public void put(String tableName, String row,
			Map<String, Map<String, String>> values) throws Exception {
		if (values == null) {
			return;
		}
		Connection connection = ConnectionFactory.createConnection(getConfiguration());
		HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
		System.out.println(values.toString());
		final Iterator<Entry<String, Map<String, String>>> cfIter = values
				.entrySet().iterator();
		List<Put> puts = new ArrayList<Put>();
		while (cfIter.hasNext()) {
			final Entry<String, Map<String, String>> cfEntry = cfIter.next();
			final String cfName = cfEntry.getKey();
			System.out.println("cfName----" + cfName);
			final Map<String, String> colMap = cfEntry.getValue();

			if (colMap == null) {
				continue;
			}

			Put put = new Put(Bytes.toBytes(row));
			final Iterator<Entry<String, String>> colIter = colMap.entrySet()
					.iterator();
			while (colIter.hasNext()) {
				final Entry<String, String> colEntry = colIter.next();
				String colName = colEntry.getKey();
				String value = colEntry.getValue();
				put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(colName),
						Bytes.toBytes(value));
			}
			System.out.println("ok");
			LOG.trace("Save into HBase:{}", put);
			puts.add(put);
		}
		table.put(puts);
	}

	/**
	 * @Title: getRowKeyList
	 * @Description: TODO(获取rowkey)
	 * @param @param tableName
	 * @param @param startRow
	 * @param @param endRow
	 * @param @return
	 * @param @throws IOException 设定文件
	 * @return List<String> 返回类型
	 * @throws
	 */
	public List<String> getRowKeyList(String tableName, String startRow,
			String endRow) throws IOException {
		final List<String> ret = new ArrayList<String>();
		Admin admin = null;
		Connection connection = null;
		Table table = null;
		try {
			LOG.trace("Get Row key List from HBase, startRow=:{}, endRow={}",
					startRow, endRow);
			connection = ConnectionFactory.createConnection(getConfiguration());
			admin = connection.getAdmin();
			table = connection.getTable(TableName.valueOf(tableName));

			final Scan scan = new Scan();
			scan.setCaching(getCatchSize());
			scan.setCacheBlocks(false);

			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setStopRow(Bytes.toBytes(endRow));
			scan.setFilter(new FirstKeyOnlyFilter());

			ResultScanner rs = (table).getScanner(scan);
			for (Result row : rs) {
				ret.add(new String(row.getRow()));
			}
		} catch (Exception e) {
			LOG.error("Failed to get the row key list.", e);
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

		return ret;

	}

	/**
	 * @Title: getAllColumnFamily
	 * @Description: TODO(获取列族)
	 * @param @param tableName
	 * @param @return 设定文件
	 * @return List<String> 返回类型
	 * @throws
	 */
	public List<String> getAllColumnFamily(String tableName) {
		Connection connection = null;
		Admin admin = null;

		List<String> list = new ArrayList<String>();
		try {
			connection = ConnectionFactory.createConnection(getConfiguration());
			admin = connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				HTableDescriptor newtd = admin.getTableDescriptor(TableName
						.valueOf(tableName));
				Collection<HColumnDescriptor> colF = newtd.getFamilies();
				for (HColumnDescriptor hColumnDescriptor : colF) {
					list.add(hColumnDescriptor.getNameAsString());
				}
			}
		} catch (IOException e1) {
			System.out.println("链接失败");
			e1.printStackTrace();
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

		return list;
	}

	/**
	 * @Title: deleteColumnFamily
	 * @Description: TODO(删除列族)
	 * @param @param tableName
	 * @param @param columnFamily
	 * @param @return 设定文件
	 * @return boolean 返回类型
	 * @throws
	 */
	public boolean deleteColumnFamily(String tableName, String columnFamily) {
		Connection connection = null;
		Admin admin = null;
		try {
			connection = ConnectionFactory.createConnection(getConfiguration());
			admin = connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				admin.disableTable(TableName.valueOf(tableName));
				HTableDescriptor newtd = admin.getTableDescriptor(TableName
						.valueOf(tableName));
				newtd.removeFamily(Bytes.toBytes(columnFamily));
				admin.modifyTable(TableName.valueOf(tableName), newtd);
				;
				admin.enableTable(TableName.valueOf(tableName));
			}
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("连接失败:hbase数据库连接失败");
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

		return true;
	}

	/**
	 * @Title: addColumnFamily
	 * @Description: TODO(增加列族)
	 * @param @param tableName
	 * @param @param columnFamily
	 * @param @return 设定文件
	 * @return boolean 返回类型
	 * @throws
	 */
	public boolean addColumnFamily(String tableName, String columnFamily) {
		Connection connection = null;
		Admin admin = null;
		try {
			connection = ConnectionFactory.createConnection(getConfiguration());
			admin = connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {

				admin.disableTable(TableName.valueOf(tableName));
				HTableDescriptor newtd = admin.getTableDescriptor(TableName
						.valueOf(tableName));
				HColumnDescriptor newhcd = new HColumnDescriptor(columnFamily);
				newtd.addFamily(newhcd);
				admin.modifyTable(TableName.valueOf(tableName), newtd);
				;
				admin.enableTable(TableName.valueOf(tableName));

			}
		} catch (IOException e1) {
			e1.printStackTrace();
			System.out.println("连接失败：hbase数据库连接失败");
		} finally {
			try {
				if (admin != null) {
					admin.close();
				}

				if (connection != null && !connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

		return true;
	}

	/**
	 * @Title: get
	 * @Description: TODO(根据多个rowkey获取数据)
	 * @param @param tableName
	 * @param @param rowKeys
	 * @param @return
	 * @param @throws IOException 设定文件
	 * @return List<Map<String,Map<String,String>>> 返回类型
	 * @throws
	 */
	public List<Map<String, Map<String, String>>> get(String tableName,
			List<String> rowKeys) throws IOException {
		List<Map<String, Map<String, String>>> ret = new ArrayList<Map<String, Map<String, String>>>();
		Connection connection = null;
		Table table = null;
		try {
			connection = ConnectionFactory.createConnection(getConfiguration());
			table = connection.getTable(TableName.valueOf(tableName));

			List<Get> getList = new ArrayList<Get>();
			for (String item : rowKeys) {
				getList.add(new Get(Bytes.toBytes(item)));
			}

			Result[] results = table.get(getList);
			for (Result row : results) {
				final Map<String, Map<String, String>> oneRowInfo = new HashMap<String, Map<String, String>>();
				for (Cell cell : row.rawCells()) {
					String family = new String(CellUtil.cloneFamily(cell));
					String field = new String(CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));

					Map<String, String> valueMap = oneRowInfo.get(family);
					if (valueMap == null) {
						valueMap = new HashMap<String, String>();
						oneRowInfo.put(family, valueMap);
					}
					valueMap.put(field, value);
				}
				if(!oneRowInfo.isEmpty()){
					ret.add(oneRowInfo);
				}
			}
		} catch (Exception e) {
			LOG.error("Failed to get data from hbase.", e);
		} finally {
			if (table != null) {
				table.close();
			}
			if (connection != null&& !connection.isClosed()) {
				connection.close();
			}
		}

		return ret;
	}

	/**
	 * @Title: deleteRow
	 * @Description: TODO(根据rowkey删除数据)
	 * @param @param tbname
	 * @param @param row 设定文件
	 * @return void 返回类型
	 * @throws
	 */
	public void deleteRow(String tbname, String row) {

		Connection conn = null;
		HTable table = null;
		try {
			conn = ConnectionFactory.createConnection(getConfiguration());
			table = (HTable) conn.getTable(TableName.valueOf(tbname));
			table.delete(new Delete(row.getBytes()));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != table) {
					table.close();
				}
				if (conn != null&& !conn.isClosed()) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * @Title: getOneRowBeforeRowKey
	 * @Description: TODO(根据一个rowkey获取数据)
	 * @param @param tableName
	 * @param @param startRow
	 * @param @return
	 * @param @throws IOException 设定文件
	 * @return Map<String,Map<String,String>> 返回类型
	 * @throws
	 */
	public Map<String, Map<String, String>> getOneRowBeforeRowKey(
			String tableName, String startRow) throws IOException {
		Map<String, Map<String, String>> oneRowInfo = new HashMap<String, Map<String, String>>();

		Connection connection = null;
		HTable table = null;
		try {
			connection = ConnectionFactory.createConnection(getConfiguration());
			table = (HTable) connection.getTable(TableName.valueOf(tableName));

			final Scan scan = new Scan();
			scan.setCaching(1);
			scan.setCacheBlocks(false);

			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setFilter(new PageFilter(1));
			ResultScanner rs = table.getScanner(scan);
			for (Result row : rs) {
				for (Cell cell : row.rawCells()) {
					String family = new String(CellUtil.cloneFamily(cell));
					String field = new String(CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));

					Map<String, String> valueMap = oneRowInfo.get(family);
					if (valueMap == null) {
						valueMap = new HashMap<String, String>();
						oneRowInfo.put(family, valueMap);
					}
					valueMap.put(field, value);
				}
			}

		} catch (Exception e) {
			LOG.error("Failed to getOneRowBeforeRowKey from hbase.", e);
		} finally {
			if (table != null) {
				table.close();
			}
			if (connection != null&& !connection.isClosed()) {
				connection.close();
			}
		}

		return oneRowInfo;
	}
	// 删除表
	public void delete(String tableName) throws IOException {
		Configuration config = getConfiguration();
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		if (admin.tableExists(TableName.valueOf(tableName))) {
			try {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("Delete " + tableName + " 失败");
			}
		}
		System.out.println("Delete " + tableName + " 成功");
	}

	private WriteTableThread getTableWriteInstance(String tableName)
			throws IOException {
		return getThread(tableName, getConfiguration());

	}

	public WriteTableThread getThread(String tableName, Configuration config) {
		WriteTableThreadWrapper thread = threadMap.get(tableName);

		if (thread != null) {
			return thread.getThread();
		}

		synchronized (threadMap) {
			thread = threadMap.get(tableName);
			if (thread != null) {
				return thread.getThread();
			}

			thread = new WriteTableThreadWrapper(tableName, config,
					hbaseCommitThreadNumber);

			threadMap.put(tableName, thread);
		}
		return thread.getThread();
	}

	private class WriteTableThreadWrapper {
		private WriteTableThread[] threadArray = null;

		private WriteTableThreadWrapper(String tableName, Configuration config,
				int hbaseCommitThreadNumber) {
			this.threadArray = new WriteTableThread[hbaseCommitThreadNumber];
			for (int i = 0; i < threadArray.length; i++) {
				threadArray[i] = new WriteTableThread(tableName, config);
			}
		}

		public WriteTableThread getThread() {
			if (threadArray.length == 1) {
				return threadArray[0];
			}

			return threadArray[getRandomNumber(threadArray.length)];
		}

		Random r = new Random(System.currentTimeMillis());

		private int getRandomNumber(int maxNumber) {
			return r.nextInt(maxNumber);
		}
	}

	private class WriteTableThread implements Runnable {

		private boolean stop = false;
		private String tableName;
		private Connection connection = null;
		private Admin admin = null;
		private AtomicInteger counter = null;
		private Configuration config;

		private BlockingQueue<Put> queue = null;
		private HTable table = null;

		private WriteTableThread(String tableName, Configuration config) {
			this.tableName = tableName;
			this.config = config;
			this.counter = new AtomicInteger();

			initConnection();
		}

		private void initConnection() {
			LOG.info("Generate connection and htable instance for write purpose."
					+ tableName);
			try {
				connection = ConnectionFactory
						.createConnection(getConfiguration());
				table = (HTable) connection.getTable(TableName
						.valueOf(tableName));
				table.setWriteBufferSize(getWriteBuf() * 1024 * 1024);// in M
				counter = new AtomicInteger();
				queue = new LinkedBlockingQueue<Put>(queueSize);

			} catch (IOException e) {
				LOG.error(
						"Failed to generate hbase connection and htable instance."
								+ tableName, e);
				closeConnection();
			}
			Thread t = new Thread(this);
			t.start();
		}

		private void closeConnection() {

			LOG.info("Close connection and htable instance. " + tableName);

			try {
				if (connection != null&& !connection.isClosed()) {
					connection.close();
				}
				if (table != null) {
					table.close();
				}
			} catch (IOException e) {
				LOG.error(
						"Failed to close hbase connection and htable instance."
								+ tableName, e);
			}
		}

		public void put(Put put) throws Exception {

			if (table == null) {
				failedCounter.incrementAndGet();
				LOG.error(
						"Failed to save data into hbase because table {} is not usable.",
						tableName);
				return;
			}
			counter.addAndGet(1);

			if (commitPeriod <= 0) {
				table.put(put);
				table.flushCommits();
			} else {
				try {
					queue.add(put);
				} catch (Exception e) {
					int failedNumber = failedCounter.incrementAndGet();
					if (failedNumber % 100 == 1) {
						LOG.error(
								"Failed to add data into queue of HBASE table{}, failed number{}, is queue full?",
								tableName, failedNumber);
					}
					closeConnection();
				}
			}
			
		}

		public void put(List<Put> puts) throws Exception {
			if (table == null) {
				failedCounter.addAndGet(puts.size());
				LOG.error(
						"Failed to save data into hbase because table {} is not usable.",
						tableName);
				return;
			}

			counter.addAndGet(puts.size());

			if (commitPeriod <= 0) {
				table.put(puts);
				table.flushCommits();
			} else {
				try {
					queue.addAll(puts);
				} catch (Exception e) {
					int failedNumber = failedCounter.addAndGet(puts.size());
					LOG.error(
							"Failed to add data into queue of HBASE table{}, failed number {}, is queue full?",
							tableName, failedNumber);
				}

			}
		}

		public void run() {
			int length = 0;
			int counter = 0;
			long lastCommitTime = System.currentTimeMillis();
			final List<Put> puts = new ArrayList<Put>(hbaseBatchCommitNumber);
			while (!stop) {
				try {
					Put put = queue.take();

					queue.drainTo(puts, hbaseBatchCommitNumber - 1);
					puts.add(put);
					counter += puts.size();
					table.put(puts);
					LOG.debug(
							"Add {} records into hbase client, current length is {}.",
							puts.size(), length);

					puts.clear();

					if (counter >= (commitSize)) {
						LOG.trace(
								"Start to flush {} {} records for size condition is reached.",
								counter, tableName);
						long startTime = System.currentTimeMillis();
						table.flushCommits();
						long endTime = System.currentTimeMillis();
						LOG.info(
								"Finish flushing {} records into {} for size condition is reached, spent {} ms.");
						length = 0;
						counter = 0;
						lastCommitTime = endTime;
					} else {
						long now = System.currentTimeMillis();
						if (lastCommitTime + commitPeriod * 1000 <= now) {
							LOG.trace(
									"Start to flush {} {} records for time condition is reached.",
									counter, tableName);
							long startTime = now;
							table.flushCommits();
							long endTime = System.currentTimeMillis();
							LOG.info(
									"Finish flushing {} records into {} for time condition is reached, spent {} ms.",
									counter, tableName);
							length = 0;
							counter = 0;
							lastCommitTime = now;
						}
					}

				} catch (Exception e) {
					e.printStackTrace();
					LOG.error("Error happened in AutoFlushThread " + tableName,
							e);
					LOG.error("Try to close and then re-generate connections and htable instance "
							+ tableName);
					closeConnection();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					initConnection();
				}
			}

			closeConnection();
			
		}

		public void close() {
			stop = true;
		}
	}

	public int getWriteBuf() {
		return writeBuf;
	}

	public void setWriteBuf(int writeBuf) {
		this.writeBuf = writeBuf;
	}

	public String getHbaseRootDir() {
		return hbaseRootDir;
	}

	public void setHbaseRootDir(String hbaseRootDir) {
		this.hbaseRootDir = hbaseRootDir;
	}

	public String getHbaseZookeeperQuorum() {
		return hbaseZookeeperQuorum;
	}

	public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
		this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
	}

	public int getCommitPeriod() {
		return commitPeriod;
	}

	public void setCommitPeriod(int commitPeriod) {
		this.commitPeriod = commitPeriod;
	}

	public int getCatchSize() {
		return catchSize;
	}

	public void setCatchSize(int catchSize) {
		this.catchSize = catchSize;
	}

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public int getHbaseCommitThreadNumber() {
		return hbaseCommitThreadNumber;
	}

	public void setHbaseCommitThreadNumber(int hbaseCommitThreadNumber) {
		this.hbaseCommitThreadNumber = hbaseCommitThreadNumber;
	}

	public int getHbaseBatchCommitNumber() {
		return hbaseBatchCommitNumber;
	}

	public void setHbaseBatchCommitNumber(int hbaseBatchCommitNumber) {
		this.hbaseBatchCommitNumber = hbaseBatchCommitNumber;
	}

	public int getCommitSize() {
		return commitSize;
	}

	public void setCommitSize(int commitSize) {
		this.commitSize = commitSize;
	}

}