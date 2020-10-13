package streamWithBatch;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkToHive {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(5);
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings);
		String category_name = "hive_category";
		HiveCatalog catalog = new HiveCatalog(
				category_name, // catalog name
				"flink2Hive", // default database
				"/etc/hive/conf", // Hive config (hive-site.xml) directory
				"2.3.4" // Hive version（现在只是支持1.2.1和2.3.4两个版本）
		);
		tableEnv.registerCatalog(category_name, catalog);
		tableEnv.useCatalog(category_name);
		String[] lst = tableEnv.listTables();// ("CREATE DATABASE IF NOT EXISTS stream_tmp")
		for (String tblName : lst) {
			System.out.println(tblName);
		}
		// to use hive dialect（如果是使用Default，此处切换为Default）
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		String createDbSql = "CREATE TABLE hive_table (user_id STRING,order_amount DOUBLE) "
				+ "PARTITIONED BY (dt STRING) \n" + "STORED AS PARQUET TBLPROPERTIES (\n"
				+ "  'sink.partition-commit.trigger'='partition-time',\n"
				+ "  'partition.time-extractor.timestamp-pattern'='$dt $hour:00:00',\n"
				+ "  'sink.partition-commit.delay'='1 h',\n"
				+ "  'sink.partition-commit.policy.kind'='metastore,success-file'\n" + ")";
		tableEnv.executeSql(createDbSql);

//		tableEnv.execute("SQL Job");
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
