package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.component.ReportGenerator;
import com.rbkmoney.fistful.reporter.config.properties.ReportingProperties;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.service.ReportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class FistfulReportsScheduler {

    private final ReportingProperties reportingProperties;
    private final ReportService reportService;
    private final ReportGenerator reportGenerator;

    @Scheduled(fixedDelayString = "${reporting.pollingDelay:3000}")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processPendingReports() throws RuntimeException {
        if (reportingProperties.isPollingEnable()) {
            List<Report> reports = reportService.getPendingReports();
            for (Report report : reports) {
                reportGenerator.generateReportFile(report);
            }
        }
    }
}
