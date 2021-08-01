package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZoneOffset;
import java.util.List;

import static com.rbkmoney.fistful.reporter.data.TestData.contractId;
import static com.rbkmoney.fistful.reporter.data.TestData.partyId;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getFromTime;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getToTime;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class ReportServiceTest {

    @Autowired
    private ReportService reportService;

    @Test
    public void reportServiceTest() {
        List<Long> reportIds = range(0, 5)
                .mapToLong(i -> createReport("withdrawalRegistry"))
                .boxed()
                .collect(toList());
        reportIds.add(createReport("anotherReportType"));

        assertEquals(
                5,
                reportService.getReportsByRange(
                        partyId,
                        contractId,
                        getFromTime().toInstant(ZoneOffset.UTC),
                        getToTime().toInstant(ZoneOffset.UTC),
                        singletonList("withdrawalRegistry")
                )
                        .size()
        );

        Long reportId = reportIds.get(0);
        Report report = reportService.getReport(partyId, contractId, reportId);
        assertEquals(getToTime(), report.getToTime());

        reportService.cancelReport(partyId, contractId, reportId);

        assertEquals(
                4,
                reportService.getReportsByRangeNotCancelled(
                        partyId,
                        contractId,
                        getFromTime().toInstant(ZoneOffset.UTC),
                        getToTime().toInstant(ZoneOffset.UTC),
                        singletonList("withdrawalRegistry")
                )
                        .size()
        );

        reportService.changeReportStatus(report, ReportStatus.created);

        assertEquals(
                5,
                reportService.getReportsByRangeNotCancelled(
                        partyId,
                        contractId,
                        getFromTime().toInstant(ZoneOffset.UTC),
                        getToTime().toInstant(ZoneOffset.UTC),
                        singletonList("withdrawalRegistry")
                )
                        .size()
        );

        assertEquals(5, reportService.getPendingReports().size());
    }

    private long createReport(String reportType) {
        return reportService.createReport(
                partyId,
                contractId,
                getFromTime().toInstant(ZoneOffset.UTC),
                getToTime().toInstant(ZoneOffset.UTC),
                reportType
        );
    }
}
