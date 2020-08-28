package kafkatoflink;

import java.util.Map;

public class Metric {
    public String name;
    public long timestamp;
    public String url;
    public Integer count;
    public Map<String, Object> fields;
    public Map<String, String> tags;
    public Metric(){}
}