package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.ReportDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.records.ReportRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

import static com.rbkmoney.fistful.reporter.domain.tables.Report.REPORT;

@Component
public class ReportDaoImpl extends AbstractGenericDao implements ReportDao {

    private final RecordRowMapper<Report> reportRowMapper;

    public ReportDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        reportRowMapper = new RecordRowMapper<>(REPORT, Report.class);
    }

    @Override
    public long save(Report report) {
        ReportRecord record = getDslContext().newRecord(REPORT, report);
        Query query = getDslContext().insertInto(REPORT).set(record).returning(REPORT.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public List<Report> getReportsByRange(String partyId, String contractId, LocalDateTime fromTime, LocalDateTime toTime, List<String> reportTypes) {
        Condition condition = REPORT.PARTY_ID.eq(partyId)
                .and(REPORT.CONTRACT_ID.eq(contractId))
                .and(REPORT.FROM_TIME.ge(fromTime))
                .and(REPORT.TO_TIME.le(toTime));

        if (!reportTypes.isEmpty()) {
            // https://github.com/rbkmoney/fistful-reporter-proto/blob/e5157befc28943c9c60d22a8fb2f18c23bf1d48f/proto/fistful_reporter.thrift#L83
            condition = condition.and(REPORT.TYPE.in(reportTypes));
        }

        Query query = getDslContext().selectFrom(REPORT).where(condition);

        return fetch(query, reportRowMapper);
    }

    @Override
    public Report getReport(long reportId, String partyId, String contractId) {
        Condition condition = REPORT.ID.eq(reportId)
                .and(REPORT.PARTY_ID.eq(partyId))
                .and(REPORT.CONTRACT_ID.eq(contractId));
        Query query = getDslContext().selectFrom(REPORT).where(condition);

        return fetchOne(query, reportRowMapper);
    }

    @Override
    public void changeReportStatus(Long reportId, ReportStatus reportStatus) {
        Condition condition = REPORT.ID.eq(reportId);
        Query query = getDslContext().update(REPORT).set(REPORT.STATUS, reportStatus).where(condition);

        executeOne(query);
    }

    @Override
    public List<Report> getPendingReports() {
        Condition condition = REPORT.STATUS.eq(ReportStatus.pending);
        Query query = getDslContext().selectFrom(REPORT).where(condition).forUpdate();

        return fetch(query, reportRowMapper);
    }

    @Override
    public Report getFirstPendingReport() {
        Query query = getDslContext().selectFrom(REPORT)
                .where(REPORT.STATUS.eq(ReportStatus.pending))
                .limit(1)
                .forUpdate();

        return fetchOne(query, reportRowMapper);
    }
}
