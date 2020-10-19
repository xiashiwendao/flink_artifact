package streamWithBatch;

import java.time.Duration;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * 代码参考：
 * https://developer.aliyun.com/article/768720
 * 
 * @author lorry
 *
 */
public class FlinkToHive {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(5);
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings);
		String category_name = "hive_category";
		HiveCatalog catalog = new HiveCatalog(category_name, // catalog name
				"flink2hive", // default database
				"/etc/hive/conf", // Hive config (hive-site.xml) directory
				"1.2.1" // Hive version（现在只是支持1.2.1和2.3.4两个版本）
		);
		
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));

		
		tableEnv.registerCatalog(category_name, catalog);
		tableEnv.useCatalog(category_name);
		
		/**
		 * 创建Hive相关的表
		 */
		tableEnv.useDatabase("flink2hive");
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		String table_name = "hive_table";
		String createDbSql = "CREATE TABLE " + table_name + " (user_id STRING,order_amount DOUBLE) "
				+ "PARTITIONED BY (dt STRING) \n" + "STORED AS PARQUET TBLPROPERTIES (\n"
				+ "  'sink.partition-commit.trigger'='partition-time',\n"
				+ "  'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',\n"
				+ "  'sink.partition-commit.delay'='1 h',\n"
				+ "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" + ")";
		ifNotExistCreateTable(tableEnv, table_name, createDbSql);
//		tableEnv.executeSql("DROP TABLE IF EXISTS " + table_name);
		
		
		table_name = "analytics_access_log_hive";
		String createHiveLogTbl = "CREATE TABLE " + table_name + " (\n" + 
				"  ts BIGINT,\n" + 
				"  user_id BIGINT,\n" + 
				"  event_type STRING,\n" + 
				"  from_type STRING,\n" + 
				"  column_type STRING,\n" + 
				"  site_id BIGINT,\n" + 
				"  groupon_id BIGINT,\n" + 
				"  partner_id BIGINT,\n" + 
				"  merchandise_id BIGINT\n" + 
				") PARTITIONED BY (\n" + 
				"  ts_date STRING,\n" + 
				"  ts_hour STRING,\n" + 
				"  ts_minute STRING\n" + 
				") STORED AS PARQUET\n" + 
				"TBLPROPERTIES (\n" + 
				"  'sink.partition-commit.trigger' = 'partition-time',\n" + 
				"  'sink.partition-commit.delay' = '1 min',\n" + 
				"  'sink.partition-commit.policy.kind' = 'metastore,success-file',\n" + 
				"  'partition.time-extractor.timestamp-pattern' = '$ts_date $ts_hour:$ts_minute:00'\n" + 
				")";
		
		ifNotExistCreateTable(tableEnv, table_name, createHiveLogTbl);
		
		/**
		 * 创建kafka表
		 */
		tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
		tableEnv.useDatabase("stream_tmp");
		table_name = "kafka_table";
		String createKafkaTbl = "CREATE TABLE kafka_table (\n" + "  user_id STRING,\n"
				+ "  order_amount DOUBLE,\n" + "  log_ts TIMESTAMP(3),\n"
				+ "  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND\n" + ")";
		
		ifNotExistCreateTable(tableEnv, table_name, createKafkaTbl);

		table_name = "analytics_access_log_kafka";
		String createKafkaLogTbl = " CREATE TABLE analytics_access_log_kafka (\n" + "   ts BIGINT,\n"
				+ "   userId BIGINT,\n" + "   eventType STRING,\n" + "   fromType STRING,\n" + "   columnType STRING,\n"
				+ "   siteId BIGINT,\n" + "   grouponId BIGINT,\n" + "   partnerId BIGINT,\n"
				+ "   merchandiseId BIGINT,\n" + "   procTime AS PROCTIME(),\n"
				+ "   eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n"
				+ "   WATERMARK FOR eventTime AS eventTime - INTERVAL '15' SECOND\n" + " ) WITH (\n"
				+ "   'connector' = 'kafka',\n" + "   'topic' = 'ods_analytics_access_log',\n"
				+ "   'properties.bootstrap.servers' = '172.16.103.96:9092',\n"
				+ "   'properties.zookeeper.connect' = '172.16.103.90:2181',\n"
				+ "   'properties.group.id' = 'flink_hive_integration_exp_1',\n"
				+ "   'scan.startup.mode' = 'latest-offset',\n" + "   'format' = 'json',\n"
				+ "   'json.fail-on-missing-field' = 'false',\n" + "   'json.ignore-parse-errors' = 'true'\n" + " )";
		
		ifNotExistCreateTable(tableEnv, table_name, createKafkaLogTbl);
		tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
		tableEnv.useDatabase("flink2hive");
		String insertSql = "INSERT INTO flink2hive.analytics_access_log_hive\n" + 
				"SELECT\n" + 
				"  ts,userId,eventType,fromType,columnType,siteId,grouponId,partnerId,merchandiseId,\n" + 
				"  DATE_FORMAT(eventTime,'yyyy-MM-dd'),\n" + 
				"  DATE_FORMAT(eventTime,'HH'),\n" + 
				"  DATE_FORMAT(eventTime,'mm')\n" + 
				"FROM stream_tmp.analytics_access_log_kafka\n" + 
				"WHERE merchandiseId > 0";
		
		tableEnv.executeSql(insertSql);
//		System.out.println("+++++++ try to select analytics_access_log_kafka ++++++");
//
//		String selectSql = "select * from stream_tmp.analytics_access_log_kafka";
//		TableResult tr = tableEnv.executeSql(selectSql);
//		Table t = tableEnv.sqlQuery(selectSql);
//		TableResult result = t.execute();
//		CloseableIterator<Row> iter = tr.collect();
//		while(iter.hasNext()) {
//			Row row = iter.next();
//			Object o1 = row.getField(0);
//			System.out.println("+++++++ find one record ++++++");
//		}
		tableEnv.execute("SQL Job");
	}
	
	private static boolean ifNotExistCreateTable(StreamTableEnvironment tableEnv, String table_name, String createSql) {
		boolean create_flag = false;
		if(!exists_check(tableEnv,table_name)) {
			tableEnv.executeSql(createSql);
			
			create_flag = true;
		}
		
		return create_flag;
	}

	/**
	 * 如果表不存在则创建，Hive Dialect并不支持IF NOT EXISTS语句

	 * @param tableEnv
	 * @param table_name
	 * @return
	 */
	private static boolean exists_check(StreamTableEnvironment tableEnv, String table_name) {
		String[] lst = tableEnv.listTables();// ("CREATE DATABASE IF NOT EXISTS stream_tmp")
		boolean exists_flag = false;
		for (String tblName : lst) {
			if (tblName.equals(table_name)) {
				exists_flag = true;
				break;
			}
		}

		return exists_flag;
	}

	private static void sql2() {
//		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//		// ingest a DataStream from an external source
//		DataStream<Tuple3<Long, String, Integer>> ds = env.addSource(...);
//
//		// SQL query with an inlined (unregistered) table
//		Table table = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
//		Table result = tableEnv.sqlQuery(
//		  "SELECT SUM(amount) FROM " + table + " WHERE product LIKE '%Rubber%'");
//
//		// SQL query with a registered table
//		// register the DataStream as view "Orders"
//		tableEnv.createTemporaryView("Orders", ds, $("user"), $("product"), $("amount"));
//		// run a SQL query on the Table and retrieve the result as a new Table
//		Table result2 = tableEnv.sqlQuery(
//		  "SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
//
//		// create and register a TableSink
//		final Schema schema = new Schema()
//		    .field("product", DataTypes.STRING())
//		    .field("amount", DataTypes.INT());
//
//		tableEnv.connect(new FileSystem().path("/path/to/file"))
//		    .withFormat(...)
//		    .withSchema(schema)
//		    .createTemporaryTable("RubberOrders");
//
//		// run an INSERT SQL on the Table and emit the result to the TableSink
//		tableEnv.executeSql(
//		  "INSERT INTO RubberOrders SELECT product, amount FROM Orders WHERE product LIKE '%Rubber%'");
	}

	private static void sql1() throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//	        TableEnvironment tEnv = TableEnvironment.create(settings);

		StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		bsEnv.setParallelism(1);
		bsEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(bsEnv, settings);
		String ddl = "CREATE TABLE user_log (\n" + "    userId BIGINT,\n" + "    itemId BIGINT,\n"
				+ "    categoryId BIGINT,\n" + "    behavior STRING,\n" + "    ts TIMESTAMP(3),\n"
				+ "    t as TO_TIMESTAMP(FROM_UNIXTIME(itemId / 1000,'yyyy-MM-dd HH:mm:ss')),"
				+ "    proctime as PROCTIME(),\n" + "    WATERMARK FOR t as t - INTERVAL '0.001' SECOND \n"
				+ ") WITH (\n" + "    'connector.type' = 'kafka',\n" + "    'connector.version' = 'universal',\n"
				+ "    'connector.topic' = 'myTest1',\n" + "    'connector.startup-mode' = 'latest-offset',\n"
				+ "    'connector.properties.0.key' = 'zookeeper.connect',\n"
				+ "    'connector.properties.0.value' = 'localhost:2181',\n"
				+ "    'connector.properties.1.key' = 'bootstrap.servers',\n"
				+ "    'connector.properties.1.value' = 'localhost:9092',\n" + "    'update-mode' = 'append',\n"
				+ "    'format.type' = 'json'\n" +
//	                "    'format.derive-schema' = 'true'\n" +
				")";

		String ddl2 = "SET table.sql-dialect=hive;\n"
				+ "CREATE TABLE hive_table (user_id STRING,order_amount DOUBLE) PARTITIONED BY (dt STRING, hour STRING) STORED AS PARQUET TBLPROPERTIES (\n"
				+ "  'sink.partition-commit.trigger'='partition-time',\n"
				+ "  'partition.time-extractor.timestamp-pattern'=’$dt $hour:00:00’,\n"
				+ "  'sink.partition-commit.delay'='1 h',\n"
				+ "  'sink.partition-commit.policy.kind’='metastore,success-file'\n" + ")";
		tableEnv.sqlUpdate(ddl2);
//
//		String ddlMysql = "CREATE TABLE pvuv_sink (\n" + "    dt TIMESTAMP(3),\n" + "    pv BIGINT,\n"
//				+ "    uv BIGINT\n" + ") WITH (\n" + "    'connector.type' = 'jdbc',\n"
//				+ "    'connector.url' = 'jdbc:mysql://localhost:3306/dota2',\n"
//				+ "    'connector.table' = 'pvuv_sink',\n" + "    'connector.username' = 'root',\n"
//				+ "    'connector.password' = '',\n" + "    'connector.write.flush.max-rows' = '1'\n" + ")";
//		tableEnv.sqlUpdate(ddlMysql);
//
//		String dml = "INSERT INTO pvuv_sink \n" + "SELECT\n" + "  TUMBLE_START(t, INTERVAL '1' MINUTE) AS dt,\n"
//				+ "  count(categoryId) AS pv,\n" + "  userId AS uv\n"
//				+ "FROM user_log GROUP BY TUMBLE(t, INTERVAL '1' MINUTE), userId";
//		tableEnv.sqlUpdate(dml);
		tableEnv.execute("SQL Job");
	}

}
