package com.rbkmoney.fistful.reporter.config;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.reporter.dao.impl.FileInfoDaoImpl;
import com.rbkmoney.fistful.reporter.service.EventServiceTests;
import com.rbkmoney.fistful.reporter.service.impl.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ContextConfiguration(
        classes = EventServiceTests.Config.class,
        initializers = AbstractEventServiceConfig.Initializer.class
)
@TestPropertySource("classpath:application.yml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@Slf4j
public abstract class AbstractEventServiceConfig extends AbstractTestUtils {

    public static class Initializer extends ConfigFileApplicationContextInitializer {

        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            super.initialize(configurableApplicationContext);
        }
    }

    @ComponentScan(
            basePackages = {
                    "com.rbkmoney.fistful.reporter.poller",
                    "com.rbkmoney.fistful.reporter.service"
            },
            excludeFilters = {
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = FileStorageServiceImpl.class),
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = PartyManagementServiceImpl.class),
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = ReportServiceImpl.class),
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = WithdrawalRegistryTemplateServiceImpl.class),
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = EventServiceTests.class),
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = FileInfoDaoImpl.class),
                    @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = FileInfoServiceImpl.class),
            }
    )
    @TestConfiguration
    public static class Config {

    }
}
