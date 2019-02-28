package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.TestContainers;
import org.springframework.boot.test.util.TestPropertyValues;

import java.util.ArrayList;
import java.util.List;

import static com.rbkmoney.TestContainersConstants.FILE_STORAGE_PORT;

public class FistfulReporterTestPropertyValuesBuilder {

    public static TestPropertyValues build(TestContainers testContainers) {
        List<String> strings = new ArrayList<>();
        if (!testContainers.isDockerContainersEnable()) {
            withUsingTestContainers(testContainers, strings);
        } else {
            withoutUsingTestContainers(strings);
        }

        strings.add("eventstock.pollingEnable=false");
        strings.add("reporting.pollingEnable=false");
        return TestPropertyValues.of(strings);
    }

    private static void withUsingTestContainers(TestContainers testContainers, List<String> strings) {
        testContainers.getPostgresSQLTestContainer().ifPresent(
                c -> {
                    strings.add("spring.datasource.url=" + c.getJdbcUrl());
                    strings.add("spring.datasource.username=" + c.getUsername());
                    strings.add("spring.datasource.password=" + c.getPassword());
                    strings.add("flyway.url=" + c.getJdbcUrl());
                    strings.add("flyway.user=" + c.getUsername());
                    strings.add("flyway.password=" + c.getPassword());
                }
        );
        testContainers.getFileStorageTestContainer().ifPresent(
                c -> {
                    strings.add("filestorage.url=http://" + c.getContainerIpAddress() + ":" + FILE_STORAGE_PORT + "/file_storage");
                }
        );
    }

    private static void withoutUsingTestContainers(List<String> strings) {
        // FILE_STORAGE_PORT должен совпадать с портом из docker-compose-dev.yml
        strings.add("filestorage.url=http://localhost:" + FILE_STORAGE_PORT + "/file_storage");
    }
}
