package com.rbkmoney;

import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.rnorth.ducttape.TimeoutException;
import org.rnorth.ducttape.ratelimits.RateLimiter;
import org.rnorth.ducttape.ratelimits.RateLimiterBuilder;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.rbkmoney.TestContainersConstants.*;
import static org.rnorth.ducttape.unreliables.Unreliables.retryUntilSuccess;

@NoArgsConstructor
@Setter
@Slf4j
public class TestContainers {

    private Boolean dockerContainersEnable;
    private PostgreSQLContainer postgresSQLTestContainer;
    private GenericContainer cephTestContainer;
    private GenericContainer fileStorageTestContainer;

    public Optional<PostgreSQLContainer> getPostgresSQLTestContainer() {
        return Optional.ofNullable(postgresSQLTestContainer);
    }

    public Optional<GenericContainer> getCephTestContainer() {
        return Optional.ofNullable(cephTestContainer);
    }

    public Optional<GenericContainer> getFileStorageTestContainer() {
        return Optional.ofNullable(fileStorageTestContainer);
    }

    public Boolean isDockerContainersEnable() {
        return dockerContainersEnable;
    }

    public void startTestContainers() {
        if (!isDockerContainersEnable()) {
            getPostgresSQLTestContainer().ifPresent(
                    container -> {
                        container.withStartupTimeout(Duration.ofMinutes(1));
                        log.info("Starting postgres container");
                        container.start();
                        log.info("Postgres container successfully started");
                    }
            );
            getCephTestContainer().ifPresent(
                    container -> {
                        container
                                .withNetworkAliases("ceph-test-container")
                                .withExposedPorts(5000, 80)
                                .withEnv("RGW_NAME", "localhost")
                                .withEnv("NETWORK_AUTO_DETECT", "4")
                                .withEnv("CEPH_DEMO_UID", "ceph-test")
                                .withEnv("CEPH_DEMO_ACCESS_KEY", AWS_ACCESS_KEY)
                                .withEnv("CEPH_DEMO_SECRET_KEY", AWS_SECRET_KEY)
                                .withEnv("CEPH_DEMO_BUCKET", BUCKET_NAME)
                                .waitingFor(getWaitStrategy("/api/v0.1/health", 200, Duration.ofMinutes(1)));
                        log.info("Starting ceph container");
                        container.start();
                        log.info("Ceph container successfully started");
                    }
            );
            getFileStorageTestContainer().ifPresent(
                    container -> {
                        container
                                .withNetworkAliases("file-storage-test-container")
                                // это не сработает при тестах на mac os. но этот контейнер нужен только при инетграционных тестах с файловым хранилищем
                                .withNetworkMode("host")
                                .withEnv("storage.endpoint", "localhost:" + getCephTestContainer().get().getMappedPort(80))
                                .withEnv("storage.signingRegion", SIGNING_REGION)
                                .withEnv("storage.accessKey", AWS_ACCESS_KEY)
                                .withEnv("storage.secretKey", AWS_SECRET_KEY)
                                .withEnv("storage.clientProtocol", PROTOCOL)
                                .withEnv("storage.clientMaxErrorRetry", MAX_ERROR_RETRY)
                                .withEnv("storage.bucketName", BUCKET_NAME)
                                .withEnv("server.port", String.valueOf(FILE_STORAGE_PORT))
                                .waitingFor(getNetworkModeHostWaitStrategy("/actuator/health", 200, FILE_STORAGE_PORT, Duration.ofMinutes(1)));
                        log.info("Starting file-storage container");
                        container.start();
                        log.info("File-storage container successfully started");
                    }
            );
        }
    }

    public void stopTestContainers() {
        if (!isDockerContainersEnable()) {
            getFileStorageTestContainer().ifPresent(GenericContainer::stop);
            getCephTestContainer().ifPresent(GenericContainer::stop);
            getPostgresSQLTestContainer().ifPresent(GenericContainer::stop);
        }
    }

    private WaitStrategy getWaitStrategy(String path, Integer statusCode, Duration duration) {
        return new HttpWaitStrategy()
                .forPath(path)
                .forStatusCode(statusCode)
                .withStartupTimeout(duration);
    }

    private WaitStrategy getNetworkModeHostWaitStrategy(String path, Integer statusCode, Integer port, Duration duration) {
        return NetworkModeHostWaitStrategy.builder()
                .path(path)
                .port(port)
                .statusCode(statusCode)
                .startupTimeout(duration)
                .build();
    }

    @Builder
    @Slf4j
    private static class NetworkModeHostWaitStrategy implements WaitStrategy {

        private static final RateLimiter LIMITER = RateLimiterBuilder
                .newBuilder()
                .withRate(1, TimeUnit.SECONDS)
                .withConstantThroughput()
                .build();

        private String path = "/";
        private Integer statusCode = 200;
        private Integer port = 8022;
        private Duration startupTimeout = Duration.ofSeconds(60);

        @Override
        public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {
            InspectContainerResponse containerInfo = waitStrategyTarget.getContainerInfo();
            String name = containerInfo.getName();
            String uri = URI.create("http://" + waitStrategyTarget.getContainerIpAddress() + ":" + port + path).toString();
            log.info(String.format("%s: Waiting for %s seconds for URL: %s", name, startupTimeout.getSeconds(), uri));
            try {
                retryUntilSuccess(
                        (int) startupTimeout.getSeconds(),
                        TimeUnit.SECONDS,
                        () -> {
                            LIMITER.doWhenReady(
                                    () -> {
                                        try {
                                            final HttpURLConnection connection = (HttpURLConnection) new URL(uri).openConnection();
                                            connection.setRequestMethod("GET");
                                            connection.connect();
                                            log.info(String.format("Get response code %s ", connection.getResponseCode()));

                                            if (connection.getResponseCode() != statusCode) {
                                                throw new RuntimeException(String.format("HTTP response code was: %s", connection.getResponseCode()));
                                            }
                                        } catch (IOException e) {
                                            throw new RuntimeException(e);
                                        }
                                    }
                            );
                            return true;
                        }
                );
            } catch (TimeoutException e) {
                throw new ContainerLaunchException(String.format("Timed out waiting for URL to be accessible (%s should return HTTP %s)", uri, statusCode));
            }
        }

        @Override
        public WaitStrategy withStartupTimeout(Duration startupTimeout) {
            this.startupTimeout = startupTimeout;
            return this;
        }
    }
}
