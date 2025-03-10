package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclOperationFilter;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPatternTypeFilter;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionTypeFilter;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.auth.models.AclResourceTypeFilter;
import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

/**
 * Test different Kafka ACLs combination with different users and service account.
 * <p>
 * Tested operations:
 * <ul>
 *     <li> Produce messages
 *     <li> Consume messages
 *     <li> List topics
 *     <li> Create topic
 *     <li> Delete topic
 *     <li> List consumer groups
 *     <li> Delete consumer groups
 *     <li> List transactional IDs
 *     <li> List ACLs
 *     <li> Create ACL
 *     <li> Delete ACL
 * </ul>
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> SECONDARY_USERNAME
 *     <li> SECONDARY_PASSWORD
 *     <li> ALIEN_USERNAME
 *     <li> ALIEN_PASSWORD
 * </ul>
 */
public class KafkaAccessMgmtTest extends TestBase {
    //TODO KafkaInstanceAPITest (migrate all permission tests)

    private static final Logger LOGGER = LogManager.getLogger(KafkaAccessMgmtTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ac-" + Environment.LAUNCH_KEY;
    private static final String PRIMARY_SERVICE_ACCOUNT_NAME = "mk-e2e-ac-primary-sa-" + Environment.LAUNCH_KEY;

    private static final String TEST_TOPIC_NAME = "test-topic-01";
    private static final String TEST_TOPIC_02_NAME = "test-topic-02";
    private static final String TEST_TOPIC_03_NAME = "test-topic-03";

    private static final String TEST_TOPIC_PREFIX = "prefix-1-";
    private static final String TEST_TOPIC_WITHOUT_PREFIX_NAME = "test-topic-03";
    private static final String TEST_TOPIC_WITH_PREFIX_NAME = TEST_TOPIC_PREFIX + "test-topic-04";

    private static final String TEST_CONSUMER_GROUP_NAME_01 = "test-consumer-group-01";

    private ApplicationServicesApi primaryAPI;
    private ApplicationServicesApi secondaryAPI;
    private ApplicationServicesApi alienAPI;
    private ApplicationServicesApi adminAPI;

    private ServiceAccount primaryServiceAccount;

    private KafkaRequest kafka;
    private KafkaInstanceApi primaryKafkaInstanceAPI;
    private KafkaInstanceApi secondaryKafkaInstanceAPI;
    private KafkaInstanceApi adminKafkaInstanceAPI;

    private KafkaAdmin primaryApacheKafkaAdmin;

    private List<AclBinding> defaultPermissionsList;

    private KafkaProducerClient<String, String> primaryKafkaProducer;
    private KafkaConsumerClient<String, String> primaryKafkaConsumer;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.ADMIN_USERNAME, "the ADMIN_USERNAME env is null");
        assertNotNull(Environment.ADMIN_PASSWORD, "the ADMIN_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.SECONDARY_USERNAME, "the SECONDARY_USERNAME env is null");
        assertNotNull(Environment.SECONDARY_PASSWORD, "the SECONDARY_PASSWORD env is null");
        assertNotNull(Environment.ALIEN_USERNAME, "the ALIEN_USERNAME env is null");
        assertNotNull(Environment.ALIEN_PASSWORD, "the ALIEN_PASSWORD env is null");

        // initialize the auth objects for all users
        var primaryAuth = new KeycloakLoginSession(Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD);
        var secondaryAuth = new KeycloakLoginSession(Environment.SECONDARY_USERNAME, Environment.SECONDARY_PASSWORD);
        var alienAuth = new KeycloakLoginSession(Environment.ALIEN_USERNAME, Environment.ALIEN_PASSWORD);
        var adminAuth = new KeycloakLoginSession(Environment.ADMIN_USERNAME, Environment.ADMIN_PASSWORD);

        // initialize the mgmt APIs client for all users
        primaryAPI = ApplicationServicesApi.applicationServicesApi(primaryAuth);
        secondaryAPI = ApplicationServicesApi.applicationServicesApi(secondaryAuth);
        alienAPI = ApplicationServicesApi.applicationServicesApi(alienAuth);
        adminAPI = ApplicationServicesApi.applicationServicesApi(adminAuth);

        // create a kafka instance owned by the primary user
        LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(primaryAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);

        // create a service account owned by the primary user
        primaryServiceAccount = SecurityMgmtAPIUtils.applyServiceAccount(
            primaryAPI.securityMgmt(), PRIMARY_SERVICE_ACCOUNT_NAME);

        // create the apache kafka admin client
        primaryApacheKafkaAdmin = new KafkaAdmin(
            kafka.getBootstrapServerHost(),
            primaryServiceAccount.getClientId(),
            primaryServiceAccount.getClientSecret());
        LOGGER.info("kafka admin api initialized for instance: {}", kafka.getBootstrapServerHost());

        // initialize the Kafka Instance API (rest) clients for all users
        primaryKafkaInstanceAPI = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(primaryAuth, kafka));
        secondaryKafkaInstanceAPI = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(secondaryAuth, kafka));
        adminKafkaInstanceAPI = bwait(KafkaInstanceApiUtils.kafkaInstanceApi(adminAuth, kafka));

        // get the default ACLs for the Kafka instance
        // (the current ACLs of an unmodified Kafka instance are the default ACLs)
        defaultPermissionsList = KafkaInstanceApiAccessUtils.getAllACLs(primaryKafkaInstanceAPI);

        // create topic that is needed to perform some permission test (e.g., messages consumption)
        KafkaInstanceApiUtils.applyTopic(primaryKafkaInstanceAPI, TEST_TOPIC_NAME);
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void teardown() {

        if (primaryApacheKafkaAdmin != null) {
            // close KafkaAdmin
            primaryApacheKafkaAdmin.close();
        }

        if (primaryKafkaProducer != null) {
            primaryKafkaProducer.close();
        }
        if (primaryKafkaConsumer != null) {
            primaryKafkaConsumer.close();
        }

        assumeTeardown();

        if (Environment.SKIP_KAFKA_TEARDOWN) {
            // Try to swap the owner back
            try {
                KafkaMgmtApiUtils.changeKafkaInstanceOwner(adminAPI.kafkaMgmt(), kafka, Environment.PRIMARY_USERNAME);
                KafkaMgmtApiUtils.waitUntilOwnerIsChanged(primaryKafkaInstanceAPI);
            } catch (Throwable t) {
                LOGGER.warn("switching back owner error: {}", t.getMessage());
            }

            // Try to reset the ACLs both with the primary and secondary user
            try {
                KafkaInstanceApiAccessUtils.resetACLsTo(primaryKafkaInstanceAPI, defaultPermissionsList);
            } catch (Throwable t) {
                LOGGER.warn("reset ACLs error: {}", t.getMessage());
            }

            // try to clean all topics both with the primary and secondary user
            for (var topic : List.of(
                TEST_TOPIC_NAME,
                TEST_TOPIC_02_NAME,
                TEST_TOPIC_03_NAME,
                TEST_TOPIC_WITH_PREFIX_NAME,
                TEST_TOPIC_WITHOUT_PREFIX_NAME)) {

                try {
                    primaryKafkaInstanceAPI.deleteTopic(topic);
                } catch (Throwable t) {
                    LOGGER.warn("clean topic error: {}", t.getMessage());
                }
            }
        }

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(adminAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean kafka error: ", t);
        }
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(primaryAPI.securityMgmt(), PRIMARY_SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean main (primary) service account error: ", t);
        }
    }


    @Test
    @SneakyThrows
    public void testUserCanCreateTopicWithUnderscorePrefix() {

        LOGGER.info("Test user can create topics with prefix '__'");
        final String internalTopicName = "__topic";

        var payload = new NewTopicInput()
                .name(internalTopicName)
                .settings(new TopicSettings().numPartitions(1));
        primaryKafkaInstanceAPI.createTopic(payload);

        // clean topic
        LOGGER.info("Clean created topic {}", internalTopicName);
        try {
            primaryKafkaInstanceAPI.deleteTopic(internalTopicName);
        } catch (Exception e) {
            LOGGER.error("error while deleting topic {}, {}", internalTopicName, e.getMessage());
        }
    }

    @Test
    @SneakyThrows
    public void testUserCannotCreateTopicWithRedhatUnderscorePrefix() {

        LOGGER.info("Test user cannot create topics with prefix '__redhat_'");
        final String internalTopicName = "__redhat_topic";

        var payload = new NewTopicInput()
                .name(internalTopicName)
                .settings(new TopicSettings().numPartitions(1));
        //primaryKafkaInstanceAPI.createTopic(payload);
        assertThrows(ApiForbiddenException.class, () -> primaryKafkaInstanceAPI.createTopic(payload));
        // clean topic
        LOGGER.info("Clean created topic {}", internalTopicName);
        try {
            primaryKafkaInstanceAPI.deleteTopic(internalTopicName);
        } catch (Exception e) {
            LOGGER.error("error while deleting topic {}, {}", internalTopicName, e.getMessage());
        }
    }


    @Test
    @SneakyThrows
    public void testUserCannotAccessSystemTopics() {

        LOGGER.info("Test user cannot access '__consumer_offsets' and '__transaction_state'");
        var instanceApiTopics = primaryKafkaInstanceAPI.getTopics();
        var o = instanceApiTopics.getItems().stream()
                .filter(k -> "__consumer_offsets".equals(k.getName()) || "__transaction_state".equals(k.getName()))
                .findAny();
        assertTrue(o.isEmpty());
    }

    @Test
    @SneakyThrows
    public void testDefaultSecondaryUserCanReadTheKafkaInstance() {

        // The kafka instance is owned by the primary user and the secondary user is in the same organization
        // as the primary user
        LOGGER.info("Test that by default the secondary user can read the kafka instance");
        var kafkas = secondaryAPI.kafkaMgmt().getKafkas(null, null, null, null);

        LOGGER.debug(kafkas);

        var o = kafkas.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @Test
    @SneakyThrows
    public void testDefaultAlienUserCanNotReadTheKafkaInstance() {

        // The kafka instance is owned by the primary user and the alien user is not in the same organization
        // as the primary user
        LOGGER.info("Test that by default the alien user can not read the kafka instance");
        var kafkas = alienAPI.kafkaMgmt().getKafkas(null, null, null, null);

        LOGGER.debug(kafkas);

        var o = kafkas.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        assertTrue(o.isEmpty());
    }

    @Test
    public void testAlwaysForbiddenToCreateDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh create <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> primaryApacheKafkaAdmin.createDelegationToken());
    }

    @Test
    public void testAlwaysForbiddenToDescribeDelegationToken() {

        LOGGER.info("kafka-delegation-tokens.sh describe <forbidden>, script representation test");
        assertThrows(DelegationTokenDisabledException.class, () -> primaryApacheKafkaAdmin.describeDelegationToken());
    }

    @Test
    public void testAlwaysForbiddenToUncleanLeaderElection() {

        LOGGER.info("kafka-leader-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryApacheKafkaAdmin.electLeader(ElectionType.UNCLEAN, TEST_TOPIC_NAME));
    }

    @Test
    public void testAlwaysForbiddenToDescribeLogDirs() {

        LOGGER.info("kafka-log-dirs.sh --describe <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryApacheKafkaAdmin.logDirs());
    }

    @Test
    public void testAlwaysForbiddenToAlterPreferredReplicaElection() {

        LOGGER.info("kafka-preferred-replica-election.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryApacheKafkaAdmin.electLeader(ElectionType.PREFERRED, TEST_TOPIC_NAME));
    }

    @Test
    public void testAlwaysForbiddenToReassignPartitions() {

        LOGGER.info("kafka-reassign-partitions.sh <forbidden>, script representation test");
        assertThrows(ClusterAuthorizationException.class, () -> primaryApacheKafkaAdmin.reassignPartitions(TEST_TOPIC_NAME));
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultPermissionsServiceAccountCanListTopic() {

        LOGGER.info("Test that by default the service account can list topics");
        primaryApacheKafkaAdmin.listTopics();
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultPermissionsServiceAccountCanNotProduceAndConsumeMessages() {

        LOGGER.info("Test that by default the service account can not produce and consume messages");
        assertThrows(GroupAuthorizationException.class, () -> bwait(testTopic(
            Vertx.vertx(),
            kafka.getBootstrapServerHost(),
            primaryServiceAccount.getClientId(),
            primaryServiceAccount.getClientSecret(),
            TEST_TOPIC_NAME,
            1,
            10,
            100,
            KafkaAuthMethod.PLAIN)));
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testDefaultPermissionsServiceAccountCannotCreateACLs() {

        LOGGER.info("Test that by default the service account can not create ACL");
        assertThrows(ClusterAuthorizationException.class, () -> primaryApacheKafkaAdmin.addAclResource(ResourceType.TOPIC));
    }

    @Test(priority = 2)
    @SneakyThrows
    public void testGrantTopicAllTransactionIDAllConsumerGroupAllACLs() {
        LOGGER.info("Grant Topic All, Transaction ID All and Consumer group All operations to the service account");
        KafkaInstanceApiAccessUtils.applyAllowAllACLsOnResources(primaryKafkaInstanceAPI, primaryServiceAccount,
            List.of(AclResourceType.TOPIC, AclResourceType.GROUP, AclResourceType.TRANSACTIONAL_ID));

        LOGGER.debug(KafkaInstanceApiAccessUtils.getAllACLs(primaryKafkaInstanceAPI));
    }

    @Test(priority = 2, dependsOnMethods = "testGrantTopicAllTransactionIDAllConsumerGroupAllACLs")
    @SneakyThrows
    public void testServiceAccountCanCreateTopic() {
        LOGGER.info("Test that the service account can create topic");

        LOGGER.info("create kafka topic '{}'", TEST_TOPIC_02_NAME);
        primaryApacheKafkaAdmin.createTopic(TEST_TOPIC_02_NAME);

        // clean topic
        try {
            primaryKafkaInstanceAPI.deleteTopic(TEST_TOPIC_02_NAME);
        } catch (Exception e) {
            LOGGER.error("error while deleting topic {}, {}", TEST_TOPIC_02_NAME, e.getMessage());
        }
    }

    @Test(priority = 2, dependsOnMethods = "testGrantTopicAllTransactionIDAllConsumerGroupAllACLs")
    @SneakyThrows
    public void testServiceAccountCanProduceMessages() {

        // We need to create the consumer before the producer, so we can reset
        // all consumer offset to the current end so that only the new messages
        // that will be sent from the producer will be received from the consumer
        primaryKafkaConsumer = new KafkaConsumerClient<>(Vertx.vertx(),
            kafka.getBootstrapServerHost(),
            primaryServiceAccount.getClientId(),
            primaryServiceAccount.getClientSecret(),
            KafkaAuthMethod.PLAIN,
            StringDeserializer.class,
            StringDeserializer.class);
        bwait(primaryKafkaConsumer.resetToEnd(TEST_TOPIC_NAME));

        LOGGER.info("Test that the service account can produce messages to the topic '{}'", TEST_TOPIC_NAME);
        primaryKafkaProducer = new KafkaProducerClient<>(Vertx.vertx(),
            kafka.getBootstrapServerHost(),
            primaryServiceAccount.getClientId(),
            primaryServiceAccount.getClientSecret(),
            KafkaAuthMethod.PLAIN,
            StringSerializer.class,
            StringSerializer.class);

        var records = bwait(primaryKafkaProducer.sendAsync(TEST_TOPIC_NAME, List.of("Test message")));
        LOGGER.debug(records);
    }

    @Test(priority = 2, dependsOnMethods = "testServiceAccountCanProduceMessages")
    @SneakyThrows
    public void testServiceAccountCanConsumeMessages() {
        LOGGER.info("Test that the service account can consume messages from the topic '{}'", TEST_TOPIC_NAME);

        var records = bwait(primaryKafkaConsumer.subscribe(TEST_TOPIC_NAME)
            .compose(__ -> primaryKafkaConsumer.consumeMessages(1)));
        LOGGER.debug(records);
    }

    @Test(priority = 2, dependsOnMethods = "testGrantTopicAllTransactionIDAllConsumerGroupAllACLs")
    @SneakyThrows
    public void testServiceAccountCanListConsumerGroups() {

        // start the consumer group to list
        var kafkaConsumer = bwait(KafkaInstanceApiUtils.startConsumerGroup(Vertx.vertx(),
            TEST_CONSUMER_GROUP_NAME_01,
            TEST_TOPIC_NAME,
            kafka.getBootstrapServerHost(),
            primaryServiceAccount.getClientId(),
            primaryServiceAccount.getClientSecret()));
        bwait(kafkaConsumer.asyncClose());

        LOGGER.info("Test that the service account can list consumer groups");
        var exists = primaryApacheKafkaAdmin.listConsumerGroups().stream()
            .filter(c -> c.groupId().equals(TEST_CONSUMER_GROUP_NAME_01))
            .findAny();
        assertTrue(exists.isPresent());
    }


    @Test(priority = 2, dependsOnMethods = "testServiceAccountCanListConsumerGroups")
    @SneakyThrows
    public void testServiceAccountCanDeleteConsumerGroups() {
        LOGGER.info("Test that the service account can delete consumer groups");
        primaryApacheKafkaAdmin.deleteConsumerGroups(TEST_CONSUMER_GROUP_NAME_01);
    }


    @Test(priority = 3, dependsOnMethods = "testGrantTopicAllTransactionIDAllConsumerGroupAllACLs")
    @SneakyThrows
    public void testDenyTopicReadACL() {

        LOGGER.info("Deny Topic Read operation to the service account");
        var principal = KafkaInstanceApiAccessUtils.toPrincipal(primaryServiceAccount.getClientId());
        var acl = new AclBinding()
            .principal(principal)
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.DENY)
            .operation(AclOperation.READ);
        primaryKafkaInstanceAPI.createAcl(acl);

        LOGGER.debug(KafkaInstanceApiAccessUtils.getAllACLs(primaryKafkaInstanceAPI));
    }

    @Test(priority = 3, dependsOnMethods = "testDenyTopicReadACL")
    @SneakyThrows
    public void testServiceAccountCanStillProduceMessages() {

        LOGGER.info("Test that the service account can still produce messages to the topic '{}'", TEST_TOPIC_NAME);
        var records = bwait(primaryKafkaProducer.sendAsync(TEST_TOPIC_NAME, List.of("Test message 01")));
        LOGGER.debug(records);

        // close producer
        primaryKafkaProducer.close();
    }

    @Test(priority = 3, dependsOnMethods = "testServiceAccountCanStillProduceMessages")
    @SneakyThrows
    public void testServiceAccountCanNotConsumeMessages() {

        LOGGER.info("Test that the service account can not consume messages from the topic '{}' after deny topic read ACL", TEST_TOPIC_NAME);
        assertThrows(AuthorizationException.class, () -> bwait(primaryKafkaConsumer.consumeMessages(1)));

        //close consumer
        primaryKafkaConsumer.close();
    }

    @Test(priority = 4, dependsOnMethods = "testGrantTopicAllTransactionIDAllConsumerGroupAllACLs")
    @SneakyThrows
    public void testDenyTopicDeletionWithPrefixToAllUsersACLs() {

        LOGGER.info("Deny Topic Delete with Prefix '{}' to all users", TEST_TOPIC_PREFIX);
        var acl = new AclBinding()
            .principal("User:*")
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.PREFIXED)
            .resourceName(TEST_TOPIC_PREFIX)
            .permission(AclPermissionType.DENY)
            .operation(AclOperation.DELETE);
        primaryKafkaInstanceAPI.createAcl(acl);

        LOGGER.debug(KafkaInstanceApiAccessUtils.getAllACLs(primaryKafkaInstanceAPI));
    }

    @Test(priority = 4, dependsOnMethods = "testDenyTopicDeletionWithPrefixToAllUsersACLs")
    @SneakyThrows
    public void testServiceAccountCanCreateTopicWithAndWithoutPrefix() {

        LOGGER.info("Test that the service account can create the topic '{}'", TEST_TOPIC_WITHOUT_PREFIX_NAME);
        primaryApacheKafkaAdmin.createTopic(TEST_TOPIC_WITHOUT_PREFIX_NAME, 1, (short) 3);

        LOGGER.info("Test that the service account can create the topic '{}'", TEST_TOPIC_WITH_PREFIX_NAME);
        primaryApacheKafkaAdmin.createTopic(TEST_TOPIC_WITH_PREFIX_NAME, 1, (short) 3);
    }

    @Test(priority = 4, dependsOnMethods = "testServiceAccountCanCreateTopicWithAndWithoutPrefix")
    @SneakyThrows
    public void testServiceAccountCanDeleteTopicWithoutPrefix() {

        LOGGER.info("Test that the service account can delete the topic '{}'", TEST_TOPIC_WITHOUT_PREFIX_NAME);
        primaryApacheKafkaAdmin.deleteTopic(TEST_TOPIC_WITHOUT_PREFIX_NAME);
    }

    @Test(priority = 4, dependsOnMethods = "testServiceAccountCanCreateTopicWithAndWithoutPrefix")
    @SneakyThrows
    public void testServiceAccountCanNotDeleteTopicWithPrefix() {

        LOGGER.info("Test that the service account can not delete the topic '{}'", TEST_TOPIC_WITH_PREFIX_NAME);
        assertThrows(AuthorizationException.class, () -> primaryApacheKafkaAdmin.deleteTopic(TEST_TOPIC_WITH_PREFIX_NAME));

        // clean topic
        try {
            primaryKafkaInstanceAPI.deleteTopic(TEST_TOPIC_WITH_PREFIX_NAME);
        } catch (Exception e) {
            LOGGER.error("error while deleting topic {}, {}", TEST_TOPIC_02_NAME, e.getMessage());
        }
    }

    @Test(priority = 5, dependsOnMethods = "testGrantTopicAllTransactionIDAllConsumerGroupAllACLs")
    @SneakyThrows
    public void testDenyTopicDescribeConsumerGroupAllACLs() {

        var principal = KafkaInstanceApiAccessUtils.toPrincipal(primaryServiceAccount.getClientId());

        LOGGER.info("Deny Topic Describe to the service account");
        var denyTopicDescribeACL = new AclBinding()
            // for every user
            .principal(principal)
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.DENY)
            .operation(AclOperation.DESCRIBE);
        primaryKafkaInstanceAPI.createAcl(denyTopicDescribeACL);

        LOGGER.info("Deny Consumer groups All to the service account");
        var denyConsumerGroupAllACL = new AclBinding()
            .principal(principal)
            .resourceType(AclResourceType.GROUP)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.DENY)
            .operation(AclOperation.ALL);
        primaryKafkaInstanceAPI.createAcl(denyConsumerGroupAllACL);

        LOGGER.debug(KafkaInstanceApiAccessUtils.getAllACLs(primaryKafkaInstanceAPI));
    }

    @Test(priority = 5, dependsOnMethods = "testDenyTopicDescribeConsumerGroupAllACLs")
    @SneakyThrows
    public void testServiceAccountCanNotListTopics() {

        LOGGER.info("Test that the service account can not list the topics");
        assertTrue(primaryApacheKafkaAdmin.listTopics().isEmpty(), "Unauthorized account should receive an empty list of topics");
    }


    @Test(priority = 5, dependsOnMethods = "testDenyTopicDescribeConsumerGroupAllACLs")
    @SneakyThrows
    public void testServiceAccountCanNotListConsumerGroups() {

        LOGGER.info("Test that the service account can not list the consumer groups");
        assertTrue(primaryApacheKafkaAdmin.listConsumerGroups().isEmpty(), "Unauthorized account should receive an empty list of consumer groups");
    }

    @Test(priority = 5, dependsOnMethods = "testDenyTopicDescribeConsumerGroupAllACLs")
    @SneakyThrows
    public void testServiceAccountCanNotDeleteConsumerGroups() {

        LOGGER.info("Test that the service account can not delete consumer groups");
        assertThrows(AuthorizationException.class, () -> primaryApacheKafkaAdmin.deleteConsumerGroups(TEST_CONSUMER_GROUP_NAME_01));
    }

    @Test(priority = 6)
    @SneakyThrows
    public void testDefaultSecondaryUserCanListACLs() {

        LOGGER.info("Test that the secondary user by default can list ACLs");
        secondaryKafkaInstanceAPI.getAcls(null, null, null, null, null, null, null, null, null, null);
    }

    @Test(priority = 6)
    @SneakyThrows
    public void testDefaultSecondaryUserCanNotDeleteACLs() {

        LOGGER.info("Test that the secondary user by default can not delete ACLs");
        assertThrows(ApiForbiddenException.class, () ->
            secondaryKafkaInstanceAPI.deleteAcls(null, null, null, null, null, null));
    }

    @Test(priority = 6)
    @SneakyThrows
    public void testDefaultAdminUserCanNotCreateTopics() {

        LOGGER.info("Test that the admin user by default can not create the topic: {}", TEST_TOPIC_03_NAME);
        var payload = new NewTopicInput()
            .name(TEST_TOPIC_03_NAME)
            .settings(new TopicSettings().numPartitions(1));
        assertThrows(ApiForbiddenException.class, () -> adminKafkaInstanceAPI.createTopic(payload));
    }

    @Test(priority = 7)
    @SneakyThrows
    public void testGrantTopicsAllowAllToTheAdminUser() {

        LOGGER.info("Grant Topics All to the Admin user");
        var acl = new AclBinding()
            .principal(KafkaInstanceApiAccessUtils.toPrincipal(Environment.ADMIN_USERNAME))
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.ALLOW)
            .operation(AclOperation.ALL);
        primaryKafkaInstanceAPI.createAcl(acl);

        LOGGER.debug(KafkaInstanceApiAccessUtils.getAllACLs(primaryKafkaInstanceAPI));
    }

    @Test(priority = 7, dependsOnMethods = "testGrantTopicsAllowAllToTheAdminUser")
    @SneakyThrows
    public void testAdminUserCanCreateTopics() {

        LOGGER.info("Test that the admin user can create the topic: {}", TEST_TOPIC_03_NAME);
        var payload = new NewTopicInput()
            .name(TEST_TOPIC_03_NAME)
            .settings(new TopicSettings().numPartitions(1));
        adminKafkaInstanceAPI.createTopic(payload);
    }

    @Test(priority = 7, dependsOnMethods = "testAdminUserCanCreateTopics")
    @SneakyThrows
    public void testAdminUserCanDeleteTopics() {

        LOGGER.info("Test that the secondary user can delete the topic: {}", TEST_TOPIC_03_NAME);
        adminKafkaInstanceAPI.deleteTopic(TEST_TOPIC_03_NAME);
    }

    @Test(priority = 8)
    @SneakyThrows
    public void testDefaultAdminUserCanNotCreateACLs() {

        LOGGER.info("Test that the admin user can not create ACLs");
        var acl = new AclBinding()
            .principal(KafkaInstanceApiAccessUtils.toPrincipal(Environment.SECONDARY_USERNAME))
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.LITERAL)
            .resourceName("xyz")
            .permission(AclPermissionType.ALLOW)
            .operation(AclOperation.ALL);
        assertThrows(ApiForbiddenException.class, () -> adminKafkaInstanceAPI.createAcl(acl));
    }

    @Test(priority = 8)
    @SneakyThrows
    public void testDefaultAdminUserCanNotDeleteACLs() {

        LOGGER.info("Test that the admin user can not delete ACLs");
        assertThrows(ApiForbiddenException.class, () -> adminKafkaInstanceAPI.deleteAcls(
            AclResourceTypeFilter.TOPIC, "xx", null, null, null, null));
    }

    @SneakyThrows
    @Test(priority = 10)
    public void testAdminUserCanChangeTheKafkaInstanceOwner() {

        LOGGER.info("Switch the owner of kafka instance from the primary user to secondary user");
        kafka = KafkaMgmtApiUtils.changeKafkaInstanceOwner(adminAPI.kafkaMgmt(), kafka, Environment.SECONDARY_USERNAME);

        // wait until owner is changed (waiting for Rollout on Brokers)
        KafkaMgmtApiUtils.waitUntilOwnerIsChanged(secondaryKafkaInstanceAPI);
    }

    @SneakyThrows
    @Test(priority = 10, dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testSecondaryUserCanCreateACLs() {

        LOGGER.info("Test that the secondary user can create ACLs");
        // Add ACL that deny the user pino from deleting topics
        var acl = new AclBinding()
            .principal(KafkaInstanceApiAccessUtils.toPrincipal("pino"))
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.DENY)
            .operation(AclOperation.DELETE);
        secondaryKafkaInstanceAPI.createAcl(acl);
    }

    @SneakyThrows
    @Test(priority = 10, dependsOnMethods = "testSecondaryUserCanCreateACLs")
    public void testSecondaryUserCanDeleteACLs() {

        LOGGER.info("Test that the secondary user can delete ACLs");
        secondaryKafkaInstanceAPI.deleteAcls(
            AclResourceTypeFilter.TOPIC,
            "*",
            AclPatternTypeFilter.LITERAL,
            KafkaInstanceApiAccessUtils.toPrincipal("pino"),
            AclOperationFilter.DELETE,
            AclPermissionTypeFilter.DENY);
    }

    @SneakyThrows
    @Test(priority = 10, dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testPrimaryUserCanListACLs() {

        LOGGER.info("Test that the primary user can list ACLs");
        // Add ACL that deny the user pino from deleting topics
        primaryKafkaInstanceAPI.getAcls(null, null, null, null, null, null, null, null, null, null);
    }

    @SneakyThrows
    @Test(priority = 10, dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testPrimaryUserCanNotDeleteACLs() {

        LOGGER.info("Test that the primary user can not create ACLs");
        // Add ACL that deny the user pino from deleting topics
        var acl = new AclBinding()
            .principal(KafkaInstanceApiAccessUtils.toPrincipal("pino"))
            .resourceType(AclResourceType.TOPIC)
            .patternType(AclPatternType.LITERAL)
            .resourceName("*")
            .permission(AclPermissionType.DENY)
            .operation(AclOperation.DELETE);
        assertThrows(ApiForbiddenException.class, () -> primaryKafkaInstanceAPI.createAcl(acl));
    }

    @Test(priority = 11, dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testPrimaryUserCanNotDeleteTheKafkaInstance() {
        LOGGER.info("Test that the primary user (old owner) can not delete the Kafka instance");
        assertThrows(ApiNotFoundException.class, () -> primaryAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @Test(priority = 11, dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testAlienUserCanNotDeleteTheKafkaInstance() {
        LOGGER.info("Test that the aline user can not delete the Kafka instance");
        assertThrows(ApiNotFoundException.class, () -> alienAPI.kafkaMgmt().deleteKafkaById(kafka.getId(), true));
    }

    @SneakyThrows
    @Test(priority = 12, dependsOnMethods = "testAdminUserCanChangeTheKafkaInstanceOwner")
    public void testSecondaryUserCanDeleteTheKafkaInstance() {
        LOGGER.info("Test that the secondary user can delete the Kafka instance");
        KafkaMgmtApiUtils.cleanKafkaInstance(secondaryAPI.kafkaMgmt(), KAFKA_INSTANCE_NAME);
    }
}
