package io.managed.services.test.observatorium;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.kafka.common.protocol.types.Field;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Log4j2
public class ObservatoriumClient {
    private final static String TOKEN_REFRESHER_URL = "http://localhost:8085";

    private final String queryApiPrefix = "/api/metrics/v1/managedkafka/api/v1/query";

    private OkHttpClient httpClient = new OkHttpClient();

    public ObservatoriumClient() {
    }

    public QueryResult query(Query query) throws ObservatoriumException {
        return query(query.toString());
    }

    public QueryResult query(String query) throws ObservatoriumException {
        var builder = UriBuilder.fromPath(TOKEN_REFRESHER_URL);
        builder.path(queryApiPrefix);

        try {
            builder.queryParam("query", URLEncoder.encode(query, "ISO-8859-1"));
        } catch (UnsupportedEncodingException ex) {
            throw new ObservatoriumException(ex);
        }

        var uri = builder.build();
        Request request;
        try {
            request = new Request.Builder().url(uri.toURL()).build();
        } catch (MalformedURLException ex) {
            throw new ObservatoriumException(ex);
        }

        try (var response = httpClient.newCall(request).execute()) {
            // all successful requests to Observatorium return 200
            if (response.code() != 200) {
                throw new ObservatoriumException(String.format("expected 200 but got %d", response.code()));
            }

            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(response.body().string(), QueryResult.class);
        } catch (IOException ex) {
            throw new ObservatoriumException(ex);
        }
    }

    public static class Query {
        private String metric;

        private List labels = new ArrayList<>();

        public Query metric(String metric) {
            this.metric = metric;
            return this;
        }

        public Query label(String label, String value) {
            labels.add(String.format("%s='%s'", label, value));
            return this;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(metric);
            sb.append("{");
            sb.append(String.join(",", labels));
            sb.append("}");
            return sb.toString();
        }
    }
}
