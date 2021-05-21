package com.rbkmoney.fistful.reporter.util;

import com.rbkmoney.fistful.reporter.Report;
import com.rbkmoney.fistful.reporter.ReportStatus;
import com.rbkmoney.fistful.reporter.ReportTimeRange;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.util.List;

public class ThriftUtils {

    public static Report map(
            com.rbkmoney.fistful.reporter.domain.tables.pojos.Report report,
            List<String> fileDataIds) {
        Report thriftReport = new Report();
        thriftReport.setReportId(report.getId());
        thriftReport.setTimeRange(createTimeRange(report));
        thriftReport.setCreatedAt(TypeUtil.temporalToString(report.getCreatedAt()));
        thriftReport.setReportType(report.getType());
        thriftReport.setStatus(ReportStatus.valueOf(report.getStatus().getLiteral()));
        thriftReport.setFileDataIds(fileDataIds);
        return thriftReport;
    }

    private static ReportTimeRange createTimeRange(com.rbkmoney.fistful.reporter.domain.tables.pojos.Report report) {
        return new ReportTimeRange(
                TypeUtil.temporalToString(report.getFromTime()),
                TypeUtil.temporalToString(report.getToTime())
        );
    }
}
