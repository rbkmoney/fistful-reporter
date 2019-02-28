package com.rbkmoney;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import static com.rbkmoney.TestContainersConstants.*;

public class TestContainersBuilder {

    private boolean dockerContainersEnable;
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
        return this;
    }

    public TestContainersBuilder addFileStorageTestContainer() {
        fileStorageTestContainerEnable = true;
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
        if (postgreSQLTestContainerEnable) {
            testContainers.setPostgresSQLTestContainer(new PostgreSQLContainer<>("postgres:" + POSTGRESQL_IMAGE_TAG));
        }
        if (cephTestContainerEnable) {
            testContainers.setCephTestContainer(new GenericContainer<>("dr.rbkmoney.com/ceph-demo:" + CEPH_IMAGE_TAG));
        }
        if (fileStorageTestContainerEnable) {
            testContainers.setFileStorageTestContainer(new GenericContainer<>("dr.rbkmoney.com/rbkmoney/file-storage:" + FILE_STORAGE_IMAGE_TAG));
        }
        testContainers.setDockerContainersEnable(false);
    }
}
