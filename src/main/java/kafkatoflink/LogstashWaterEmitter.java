package kafkatoflink;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import com.alibaba.fastjson.JSON;

public class LogstashWaterEmitter implements AssignerWithPeriodicWatermarks<Metric> {
	private static final long serialVersionUID = 1L;
	private long currentTimestamp = Long.MIN_VALUE;

	@Override
    public long extractTimestamp(Metric event, long recordTimestamp) {
//        event = (Metric)JSON.parse(jsonStr);
		// String[] strs = jsonStr.split(",");
		System.out.println("extractTimestamp:: event timestamep is: " + event.timestamp);
        if (event.timestamp > currentTimestamp) {
            currentTimestamp = event.timestamp;
		}
		
        return currentTimestamp;
    }

	@Override
	public Watermark getCurrentWatermark() {
		long maxDelay_millsec = 2000;
		long watermark_level = this.currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : this.currentTimestamp - maxDelay_millsec ;
		System.out.println("current timestamp:" + this.currentTimestamp + "; water_mart: " + watermark_level);
		return new Watermark(watermark_level);
	}

}