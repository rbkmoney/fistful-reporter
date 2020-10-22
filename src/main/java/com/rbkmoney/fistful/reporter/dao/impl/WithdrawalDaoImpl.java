package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.domain.tables.records.WithdrawalRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static com.rbkmoney.fistful.reporter.domain.tables.Withdrawal.WITHDRAWAL;
import static org.jooq.impl.DSL.max;

@Component
public class WithdrawalDaoImpl extends AbstractGenericDao implements WithdrawalDao {

    private final RowMapper<Withdrawal> withdrawalRowMapper;

    @Autowired
    public WithdrawalDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        withdrawalRowMapper = new RecordRowMapper<>(WITHDRAWAL, Withdrawal.class);
    }

    @Override
    public Optional<Long> getLastEventId() {
        Query query = getDslContext().select(max(WITHDRAWAL.EVENT_ID)).from(WITHDRAWAL);

        return Optional.ofNullable(fetchOne(query, Long.class));
    }

    @Override
    public Long save(Withdrawal withdrawal) {
        WithdrawalRecord record = getDslContext().newRecord(WITHDRAWAL, withdrawal);
        Query query = getDslContext().insertInto(WITHDRAWAL).set(record).returning(WITHDRAWAL.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Withdrawal get(String withdrawalId) {
        Condition condition = WITHDRAWAL.WITHDRAWAL_ID.eq(withdrawalId)
                .and(WITHDRAWAL.CURRENT);
        Query query = getDslContext().selectFrom(WITHDRAWAL).where(condition);

        return fetchOne(query, withdrawalRowMapper);
    }

    @Override
    public void updateNotCurrent(String withdrawalId) {
        Condition condition = WITHDRAWAL.WITHDRAWAL_ID.eq(withdrawalId)
                .and(WITHDRAWAL.CURRENT);
        Query query = getDslContext().update(WITHDRAWAL).set(WITHDRAWAL.CURRENT, false).where(condition);

        execute(query);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<Withdrawal> getSucceededWithdrawalsByReport(Report report, Long fromId, int limit) {
        String partyId = report.getPartyId();
        String contractId = report.getContractId();
        LocalDateTime fromTime = report.getFromTime();
        LocalDateTime toTime = report.getToTime();

        Query query = getDslContext().select().from(WITHDRAWAL)
                .where(
                        WITHDRAWAL.PARTY_ID.eq(partyId)
                                .and(WITHDRAWAL.PARTY_CONTRACT_ID.eq(contractId))
                                .and(WITHDRAWAL.EVENT_TYPE.eq(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED))
                                .and(WITHDRAWAL.WITHDRAWAL_STATUS.eq(WithdrawalStatus.succeeded))
                                .and(WITHDRAWAL.EVENT_CREATED_AT.ge(fromTime))
                                .and(WITHDRAWAL.EVENT_CREATED_AT.le(toTime))
                                .and(WITHDRAWAL.ID.gt(fromId))
                                .and(WITHDRAWAL.CURRENT)
                )
                .orderBy(WITHDRAWAL.ID)
                .limit(limit);

        return fetch(query, withdrawalRowMapper);
    }
}
