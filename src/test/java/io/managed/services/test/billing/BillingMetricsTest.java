package io.managed.services.test.billing;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.cucumber.java.hu.Ha;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
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

    public static final String TOPIC_NAME = "billing_test";

    private ObservatoriumClient observatoriumClient;

    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private Map<String, Double> snapshotValues = new HashMap<>();

    @BeforeClass
    @SneakyThrows
    public void setup() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(
                Environment.PRIMARY_USERNAME,
                Environment.PRIMARY_PASSWORD);

        this.kafkaMgmtApi = apps.kafkaMgmt();
        this.securityMgmtApi = apps.securityMgmt();
        this.observatoriumClient = new ObservatoriumClient();

        // Create Kafka Instance
        var optionalKafkaRequest = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        if (optionalKafkaRequest.isPresent()) {
            kafka = optionalKafkaRequest.get();
        } else {
            kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        }

        this.kafkaInstanceApi = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(kafka,
                Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD));

        var optionalServiceAccount = SecurityMgmtAPIUtils.getServiceAccountByName(securityMgmtApi, SERVICE_ACCOUNT_NAME);

        if (optionalServiceAccount.isPresent()) {
            serviceAccount = securityMgmtApi.resetServiceAccountCreds(optionalServiceAccount.get().getId());
        } else {
            serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        }

        createACLs(serviceAccount);
        createTopics();
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

        snapshotValues.put(kafka.getId(), result.data.result.get(0).doubleValue());
    }

    @SneakyThrows
    private void createTopics() {
        var topics = kafkaInstanceApi.getTopics();
        for (Topic topic : topics.getItems()) {
            if (topic.getName().equals(TOPIC_NAME)) {
                return;
            }
        }

        kafkaInstanceApi.createTopic(
                new NewTopicInput()
                        .name(TOPIC_NAME)
                        .settings(
                                new TopicSettings()
                                        .numPartitions(1)));
    }

    private void createACLs(ServiceAccount serviceAccount) throws ApiGenericException {
        if (serviceAccount != null) {
            var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
            KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
        }
    }
}
