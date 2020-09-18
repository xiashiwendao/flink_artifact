//package other_demo;
//
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//public class TestBroadCast {
//	public static void main(String[] args) {
//		DataStreamSource<AlertEvent> alertData = env.addSource(new FlinkKafkaConsumer<>("alert",
//		        new AlertEventSchema(),
//		        parameterTool.getProperties()));
//	}
//	
//	DataStreamSource<Rule> alarmdata = env.addSource(new GetAlarmNotifyData());
//
//	// MapState 中保存 (RuleName, Rule) ，在描述类中指定 State name
//	MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
//	            "RulesBroadcastState",
//	            BasicTypeInfo.STRING_TYPE_INFO,
//	            TypeInformation.of(new TypeHint<Rule>() {}));
//
//	// alarmdata 使用 MapStateDescriptor 作为参数广播，得到广播流
//	BroadcastStream<Rule> ruleBroadcastStream = alarmdata.broadcast(ruleStateDescriptor);
//	
//	alertData.connect(ruleBroadcastStream)
//    .process(
//        new KeyedBroadcastProcessFunction<AlertEvent, Rule>() {
//            //根据告警规则的数据进行处理告警事件
//        }
//    )
//}
