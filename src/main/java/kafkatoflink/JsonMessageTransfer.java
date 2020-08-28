package kafkatoflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

import com.alibaba.fastjson.JSON;

public class JsonMessageTransfer implements FlatMapFunction<String, Metric> {

	private static final long serialVersionUID = 1L;

	@Override
	public void flatMap(String value, Collector<Metric> out) throws Exception {
		System.out.println("json string is: " + value);
		Metric event = (Metric) JSON.parseObject(value, Metric.class);
		out.collect(event);
	}

	// @Override
	public Metric map(String value) throws Exception {
		Metric event = (Metric) JSON.parse(value);
		System.out.println("json string is: " + value);

		return event;
	}
}