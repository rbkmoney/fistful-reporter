package com.rbkmoney;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

import java.time.Duration;

public class TestContainersBuilder {

    private final String SIGNING_REGION = "RU";
    private final String AWS_ACCESS_KEY = "test";
    private final String AWS_SECRET_KEY = "test";
    private final String PROTOCOL = "HTTP";
    private final String MAX_ERROR_RETRY = "10";
    private final String BUCKET_NAME = "TEST";

    private boolean dockerContainersEnable;
    private boolean networkEnable;
    private boolean postgreSQLTestContainerEnable;
    private boolean cephTestContainerEnable;
    private boolean fileStorageTestContainerEnable;

    private TestContainersBuilder(boolean dockerContainersEnable) {
        this.dockerContainersEnable = dockerContainersEnable;
    }

    public static TestContainersBuilder builder(boolean dockerContainersEnable) {
        return new TestContainersBuilder(dockerContainersEnable);
    }

    public TestContainersBuilder addPostgreSQLTestContainer() {
        postgreSQLTestContainerEnable = true;
        return this;
    }

    public TestContainersBuilder addCephTestContainer() {
        cephTestContainerEnable = true;
        networkEnable = true;
        return this;
    }

    public TestContainersBuilder addFileStorageTestContainer() {
        fileStorageTestContainerEnable = true;
        networkEnable = true;
        return this;
    }

    public TestContainers build() {
        TestContainers testContainers = new TestContainers();

        if (!dockerContainersEnable) {
            addTestContainers(testContainers);
        } else {
            testContainers.setDockerContainersEnable(true);
        }
        return testContainers;
    }

    private void addTestContainers(TestContainers testContainers) {
        Network.NetworkImpl nt = Network.builder().build();
        if (networkEnable) {
            testContainers.setNetwork(nt);
        }
        if (postgreSQLTestContainerEnable) {
            testContainers.setPostgresSQLTestContainer(
                    new PostgreSQLContainer<>("postgres:9.6")
                            .withStartupTimeout(Duration.ofMinutes(5))
            );
        }
        if (cephTestContainerEnable && networkEnable) {
            testContainers.setCephTestContainer(
                    new GenericContainer<>("dr.rbkmoney.com/ceph-demo:latest")
                            .withEnv("RGW_NAME", "localhost")
                            .withEnv("NETWORK_AUTO_DETECT", "4")
                            .withEnv("CEPH_DEMO_UID", "ceph-test")
                            .withEnv("CEPH_DEMO_ACCESS_KEY", AWS_ACCESS_KEY)
                            .withEnv("CEPH_DEMO_SECRET_KEY", AWS_SECRET_KEY)
                            .withEnv("CEPH_DEMO_BUCKET", BUCKET_NAME)
                            .withExposedPorts(5000, 80)
                            .withNetwork(nt)
                            .withNetworkAliases("ceph-test-container")
                            .waitingFor(getWaitStrategy("/api/v0.1/health"))
            );
        }
        if (fileStorageTestContainerEnable && networkEnable) {
            testContainers.setFileStorageTestContainer(
                    //todo ba630baa85c17e9437386dafec982e4338f58663
                    new GenericContainer<>("dr.rbkmoney.com/rbkmoney/file-storage:41706f5ab71f128015dc7a9798effb0c590016ab")
                            .withEnv("storage.endpoint", "http://ceph-test-container:80")
                            .withEnv("storage.signingRegion", SIGNING_REGION)
                            .withEnv("storage.accessKey", AWS_ACCESS_KEY)
                            .withEnv("storage.secretKey", AWS_SECRET_KEY)
                            .withEnv("storage.clientProtocol", PROTOCOL)
                            .withEnv("storage.clientMaxErrorRetry", MAX_ERROR_RETRY)
                            .withEnv("storage.bucketName", BUCKET_NAME)
                            .withEnv("server.port", "8022")
                            .withExposedPorts(8022)
                            .withNetwork(nt)
                            .withNetworkAliases("file-storage-test-container")
                            .waitingFor(getWaitStrategy("/actuator/health"))
            );
        }
        testContainers.setDockerContainersEnable(false);
    }

    private WaitStrategy getWaitStrategy(String path) {
        return new HttpWaitStrategy()
                .forPath(path)
                .forStatusCode(200)
                .withStartupTimeout(Duration.ofMinutes(10));
    }
}
