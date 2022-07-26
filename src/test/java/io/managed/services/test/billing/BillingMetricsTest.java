package io.managed.services.test.billing;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.cucumber.java.hu.Ha;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.observatorium.ObservatoriumClient;
import io.managed.services.test.observatorium.ObservatoriumException;
import io.managed.services.test.observatorium.QueryResult;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertNotNull;

@Log4j2
public class BillingMetricsTest extends TestBase {
    public static final String KAFKA_INSTANCE_NAME = "mk-billing-" + Environment.LAUNCH_KEY;

    public static final String SERVICE_ACCOUNT_NAME = "mk-billing-sa-" + Environment.LAUNCH_KEY;

    private ObservatoriumClient observatoriumClient;

    private KafkaRequest kafka;

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private Map<String, Long> snapshotValues = new HashMap<>();

    @BeforeClass
    @SneakyThrows
    public void setup() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(
                Environment.PRIMARY_USERNAME,
                Environment.PRIMARY_PASSWORD);

        this.kafkaMgmtApi = apps.kafkaMgmt();
        this.observatoriumClient = new ObservatoriumClient();

        // Create Kafka Instance
        var optionalKafkaRequest = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        if (optionalKafkaRequest.isPresent()) {
            kafka = optionalKafkaRequest.get();
        } else {
            kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        }
    }

    @AfterClass
    @SneakyThrows
    public void teardown() {
        // KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
    }


    @Test(priority = 2)
    @SneakyThrows
    public void testCheckClusterHoursValue() {
        ObservatoriumClient.Query query = new ObservatoriumClient.Query();
        query
                .metric("kafka_id:strimzi_resource_state:max_over_time1h")
                .label("_id", kafka.getId());

        QueryResult result = observatoriumClient.query(query);
        Assert.assertEquals(result.status, "success");
        Assert.assertEquals(result.data.result.size(), 1);
        Assert.assertEquals(result.data.result.get(0).value(), "1");
    }

    @Test(priority = 3)
    @SneakyThrows
    public void takeMetricsSnapshot() {
        ObservatoriumClient.Query query = new ObservatoriumClient.Query();
        query
                .metric("kafka_id:haproxy_server_bytes_in_out_total:rate1h_gibibytes")
                .label("_id", kafka.getId());

        QueryResult result = observatoriumClient.query(query);
        Assert.assertEquals(result.status, "success");
        Assert.assertEquals(result.data.result.size(), 1);

        snapshotValues.put(kafka.getId(), result.data.result.get(0).longValue());
    }


    @Test(priority = 4)
    @SneakyThrows
    public void testCreateLoad() {
        ObservatoriumClient.Query query = new ObservatoriumClient.Query();
        query
                .metric("kafka_id:haproxy_server_bytes_in_out_total:rate1h_gibibytes")
                .label("_id", kafka.getId());

        QueryResult result = observatoriumClient.query(query);
        Assert.assertEquals(result.status, "success");
        Assert.assertEquals(result.data.result.size(), 1);

        snapshotValues.put(kafka.getId(), result.data.result.get(0).longValue());
    }
}
