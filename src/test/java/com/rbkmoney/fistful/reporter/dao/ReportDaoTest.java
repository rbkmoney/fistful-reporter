package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.List;

import static com.rbkmoney.fistful.reporter.data.TestData.contractId;
import static com.rbkmoney.fistful.reporter.data.TestData.partyId;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.randomListOf;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getFromTime;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getToTime;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class ReportDaoTest {

    @Autowired
    private ReportDao reportDao;

    @Test
    public void reportDaoTest() {
        Report report = random(Report.class);
        configReport(report, getFromTime(), getToTime());
        long id = reportDao.save(report);
        Report expectedReport = reportDao.getReport(id, report.getPartyId(), report.getContractId());
        assertEquals(report, expectedReport);

        List<Report> reports = randomListOf(4, Report.class);
        for (Report r : reports) {
            configReport(r, getFromTime(), getToTime());
            reportDao.save(r);
        }
        assertEquals(5, reportDao.getPendingReports().size());

        reportDao.changeReportStatus(id, ReportStatus.created);
        assertEquals(4, reportDao.getPendingReports().size());

        assertEquals(5, reportDao.getReportsByRange(
                partyId,
                contractId,
                getFromTime(),
                getToTime(),
                emptyList()).size());

        assertEquals(0, reportDao.getReportsByRange(
                partyId,
                contractId,
                getFromTime().plusMinutes(1),
                getToTime(),
                emptyList()).size());
    }

    private void configReport(Report r, LocalDateTime fromTime, LocalDateTime toTime) {
        r.setStatus(ReportStatus.pending);
        r.setFromTime(fromTime);
        r.setToTime(toTime);
        r.setPartyId(partyId);
        r.setContractId(contractId);
    }
}
