package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.config.properties.ReportingProperties;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.generator.ReportGenerator;
import com.rbkmoney.fistful.reporter.service.ReportService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
                Report report = reportService.getFirstPendingReport();
                reportGenerator.generateReportFile(report);
                log.info("Finish scheduled task for building report");
            } catch (Throwable ex) {
                log.warn("Error with scheduled task for building report", ex);
                throw ex;
            }
        }
    }
}
