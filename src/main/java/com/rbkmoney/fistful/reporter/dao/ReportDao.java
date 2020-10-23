package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.dao.GenericDao;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;

import java.time.LocalDateTime;
import java.util.List;

public interface ReportDao extends GenericDao {

    long save(Report report);

    List<Report> getReportsByRange(String partyId, String contractId, LocalDateTime fromTime, LocalDateTime toTime, List<String> reportTypes);

    Report getReport(long reportId, String partyId, String contractId);

    void changeReportStatus(Long reportId, ReportStatus reportStatus);

    List<Report> getPendingReports();

    Report getFirstPendingReport();

}
