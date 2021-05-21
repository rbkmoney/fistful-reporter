package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.config.properties.ReportingProperties;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.generator.ReportGenerator;
import com.rbkmoney.fistful.reporter.service.ReportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class FistfulReportsScheduler {

    private final ReportingProperties reportingProperties;
    private final ReportService reportService;
    private final ReportGenerator reportGenerator;

    @Scheduled(fixedDelayString = "${reporting.pollingDelay:3000}")
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void processPendingReport() {
        if (reportingProperties.isPollingEnable()) {
            try {
                log.info("Start scheduled task for building report");
                List<Report> pendingReports = reportService.getPendingReports();
                if (!pendingReports.isEmpty()) {
                    String reportIds = pendingReports.stream()
                            .map(Report::getId)
                            .map(String::valueOf)
                            .collect(Collectors.joining(", ", "[", "]"));
                    Report report = pendingReports.get(0);
                    log.info("{} reports in queue for building, reportIds={}, " +
                                    "now start report building for reportId={}",
                            pendingReports.size(), reportIds, report.getId());
                    reportGenerator.generateReportFile(report);
                }
                log.info("Finish scheduled task for building report");
            } catch (Throwable ex) {
                log.warn("Error with scheduled task for building report", ex);
                throw ex;
            }
        }
    }
}
