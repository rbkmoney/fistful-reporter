package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Deposit;
import com.rbkmoney.fistful.reporter.domain.tables.records.DepositRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.rbkmoney.fistful.reporter.domain.tables.Deposit.DEPOSIT;

@Component
public class DepositDaoImpl extends AbstractGenericDao implements DepositDao {

    private final RowMapper<Deposit> depositRowMapper;

    @Autowired
    public DepositDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        depositRowMapper = new RecordRowMapper<>(DEPOSIT, Deposit.class);
    }

    @Override
    public Optional<Long> getLastEventId() {
        Query query = getDslContext().select(DSL.max(DEPOSIT.EVENT_ID)).from(DEPOSIT);

        return Optional.ofNullable(fetchOne(query, Long.class));
    }

    @Override
    public Long save(Deposit deposit) {
        DepositRecord record = getDslContext().newRecord(DEPOSIT, deposit);
        Query query = getDslContext().insertInto(DEPOSIT).set(record).returning(DEPOSIT.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Deposit get(String depositId) {
        Condition condition = DEPOSIT.DEPOSIT_ID.eq(depositId)
                .and(DEPOSIT.CURRENT);
        Query query = getDslContext().selectFrom(DEPOSIT).where(condition);

        return fetchOne(query, depositRowMapper);
    }

    @Override
    public void updateNotCurrent(String depositId) {
        Condition condition = DEPOSIT.DEPOSIT_ID.eq(depositId)
                .and(DEPOSIT.CURRENT);
        Query query = getDslContext().update(DEPOSIT).set(DEPOSIT.CURRENT, false).where(condition);

        execute(query);
    }

}
