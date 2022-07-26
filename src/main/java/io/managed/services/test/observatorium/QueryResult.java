package io.managed.services.test.observatorium;

import org.joda.time.DateTime;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class QueryResult {
    public String status;
    public Data data;

    public static class Data {
        public String resultType;
        public List<Result> result;
    }

    public static class Result {
        public Map<String, String> metric;
        public List<Object> value;

        public DateTime time() {
            return new DateTime(Double.valueOf((double) value.get(0)).longValue() * 1000);
        }

        public String value() {
            return value.get(1).toString();
        }

        public Long longValue() {
            return Long.valueOf(value());
        }
    }
}
