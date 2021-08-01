package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.config.testconfiguration.WithdrawalTestDao;
import com.rbkmoney.fistful.reporter.data.TestData;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.service.impl.WithdrawalRegistryTemplateServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.file.Files.*;

@PostgresqlSpringBootITest
public class WithdrawalRegistryTemplateServiceTest {

    @Autowired
    private WithdrawalRegistryTemplateServiceImpl withdrawalRegistryTemplateService;

    @Autowired
    private WithdrawalTestDao withdrawalTestDao;

    @Test
    public void withdrawalRegistryTemplateServiceTest() throws IOException {
        withdrawalTestDao.saveWithdrawalsDependencies(4001);

        Report report = TestData.createReport();
        report.setTimezone("Europe/Moscow");
        Path reportFile = createTempFile(report.getType() + "_", "_report.xlsx");
        try {
            withdrawalRegistryTemplateService.processReportFileByTemplate(report, newOutputStream(reportFile));
        } finally {
            deleteIfExists(reportFile);
        }
    }
}
