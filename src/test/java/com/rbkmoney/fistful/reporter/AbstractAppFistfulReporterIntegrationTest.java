package com.rbkmoney.fistful.reporter;

import com.rbkmoney.easyway.EnvironmentProperties;
import com.rbkmoney.easyway.TestContainers;
import com.rbkmoney.easyway.TestContainersBuilder;
import com.rbkmoney.easyway.TestContainersParameters;
import com.rbkmoney.fistful.reporter.utils.AbstractWithdrawalTestUtils;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@ContextConfiguration(classes = FistfulReporterApplication.class, initializers = AbstractAppFistfulReporterIntegrationTest.Initializer.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class AbstractAppFistfulReporterIntegrationTest extends AbstractWithdrawalTestUtils {

    private static final int TIMEOUT = 555000;

    private static TestContainers testContainers = TestContainersBuilder.builderWithTestContainers(getTestContainersParametersSupplier())
            .addPostgresqlTestContainer()
            .addCephTestContainer()
            .addFileStorageTestContainer()
            .build();

    protected static Path reportFile;

    @BeforeClass
    public static void beforeClass() throws IOException {
        testContainers.startTestContainers();
        reportFile = createTempFile("test", ".xlsx");
    }

    @AfterClass
    public static void afterClass() throws IOException {
        deleteIfExists(reportFile);
        testContainers.stopTestContainers();
    }

    @TestConfiguration
    public static class TestContextConfiguration {

        @Value("${local.server.port}")
        protected int port;

        @Bean
        public ReportingSrv.Iface reportClient() throws URISyntaxException {
            return new THSpawnClientBuilder()
                    .withAddress(new URI("http://localhost:" + port + "/fistful/reports"))
                    .withNetworkTimeout(TIMEOUT)
                    .build(ReportingSrv.Iface.class);
        }
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues.of(
                    testContainers.getEnvironmentProperties(getEnvironmentPropertiesConsumer())
            )
                    .applyTo(configurableApplicationContext);
        }
    }

    private static Supplier<TestContainersParameters> getTestContainersParametersSupplier() {
        return () -> {
            TestContainersParameters testContainersParameters = new TestContainersParameters();
            testContainersParameters.setPostgresqlJdbcUrl("jdbc:postgresql://localhost:5432/fistful_reporter");

            return testContainersParameters;
        };
    }

    private static Consumer<EnvironmentProperties> getEnvironmentPropertiesConsumer() {
        return environmentProperties -> {
            environmentProperties.put("eventstock.pollingEnable", "false");
        };
    }
}
