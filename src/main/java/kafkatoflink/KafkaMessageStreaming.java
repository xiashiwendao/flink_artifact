package kafkatoflink;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import com.alibaba.fastjson.JSON;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KafkaMessageStreaming {
	static OutputTag<String> outputTag = new OutputTag<String>("late") {
		private static final long serialVersionUID = 1L;
	};

	public static final String topic = "metric"; // kafka topic，Flink 程序中需要和这个统一

	public static void main(final String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 非常关键，一定要设置启动检查点！！
		env.enableCheckpointing(5000);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		final Properties props = new Properties();
		props.setProperty("bootstrap.servers", "172.16.103.96:9092");
		props.put("zookeeper.connect", "172.16.103.90:2181");

		props.setProperty("group.id", "flink-group2");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // key 反序列化
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest"); // value 反序列化
		
		FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
		        "summary_url",                  // target topic
		        new SimpleStringSchema(),    // serialization schema
		        props); // fault-tolerance


		final FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
		consumer.assignTimestampsAndWatermarks(new WatermarkStrategy<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public WatermarkGenerator<String> createWatermarkGenerator(
					final org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context context) {
				return new WatermarkGenerator<String>() {
					long currenttimeStamp = Long.MIN_VALUE;

					@Override
					public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
						Metric metric = JSON.parseObject(event.toString(), Metric.class);
						if (metric.timestamp > currenttimeStamp) {
							currenttimeStamp = metric.timestamp;
						}
					}

					@Override
					public void onPeriodicEmit(WatermarkOutput output) {
						long maxDelay = 2000;
						Long emit_timestamp = currenttimeStamp == Long.MIN_VALUE ? currenttimeStamp
								: currenttimeStamp - maxDelay;
						output.emitWatermark(new Watermark(emit_timestamp));
						Timestamp ts = new Timestamp(emit_timestamp);
						Date ts_date = ts;
						Date dt = new Date();
						System.out.println("+++++++++" + dt + " create a water mark: " + ts_date +" ++++++++" );
					}
				};
			}
		});
		final DataStreamSource<String> dataStreamSource = env.addSource(consumer).setParallelism(1);
		dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				Metric metric = JSON.parseObject(value, Metric.class);
				Date dt = new Date();
				System.out.println("****** get data: " + dt + " " + value + " ********");
				out.collect(new Tuple2<String, Integer>(metric.url, 1));
			}
		}).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(final Tuple2<String, Integer> value) throws Exception {
				return value.f0;
			}
		}).window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						int count = 0;
						for (Tuple2<String, Integer> item : input) {
							count += 1;
						}

						out.collect(new Tuple2<String, Integer>(key, count));
						System.out.println("+++++++ url: " + key + "; count: " + count + "++++++++++");
					}
				}).map(new MapFunction<Tuple2<String, Integer>, String>(){

					private static final long serialVersionUID = 1L;

					@Override
					public String map(Tuple2<String, Integer> value) throws Exception {
						// TODO Auto-generated method stub
						return JSON.toJSONString(value);
					}
				}).addSink(myProducer); // addSink必须要紧跟着map函数，如果是单独addsink将会导致将其他原始接收到的kafka下次一并输出
		
		
//		dataStreamSource.print(); // 把从 kafka 读取到的数据打印在控制台
//		dataStreamSource.addSink(myProducer);
		env.execute("Flink add data source");

		// args[0] = "test-0921"; //传入的是kafka中的topic
		// final FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic,
		// new SimpleStringSchema(), props);
		// consumer.assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks)new
		// LogstashWaterEmitter());
		// final DataStream<Metric> dataStreamSource = env
		// .addSource(consumer)
		// .setParallelism(1)
		// .map(new JsonMessageTransfer())
		// .keyBy(new KeySelector<Metric, String>(){
		// private static final long serialVersionUID = 1L;

		// @Override
		// public String getKey(Metric value) throws Exception {
		// return value.url;
		// }
		// });
		// .timeWindow(Time.seconds(2))
		// .reduce(new ReduceFunction<Metric>() {
		// @Override
		// public Metric reduce(Metric value1, Metric value2) throws Exception {
		// System.out.println(" value 1 count is: " + value1.count + "; value2 count: "
		// + value2.count);
		// Metric ret = new Metric();
		// ret.url = value1.url;
		// ret.count += value1.count + 1;
		// return ret;
		// }
		// });
		// .sideOutputLateData(outputTag);

		// consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());
		// DataStream<Tuple2<String, Long>> keyedStream = env
		// .addSource(consumer);
		// .flatMap(new MessageSplitter())
		// .keyBy(0)
		// .timeWindow(Time.seconds(2))
		// .apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, Tuple,
		// TimeWindow>() {
		// public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String,
		// Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
		// long sum = 0L;
		// int count = 0;
		// for (Tuple2<String, Long> record: input) {
		// sum += record.f1;
		// count++;
		// }
		// Tuple2<String, Long> result = input.iterator().next();
		// result.f1 = sum / count;
		// out.collect(result);
		// System.out.println("result key: "+result.f0 + "; value:" + result.f1);
		// }
		// });

		// 将结果打印出来
		// dataStreamSource.print();
		// 将结果保存到文件中
		// args[1] = "E:\\FlinkTest\\KafkaFlinkTest";//传入的是结果保存的路径
		// keyedStream.writeAsText("/opt/KafkaFlinkTest.txt");
		// env.execute("Kafka-Flink Test");
	}
}
