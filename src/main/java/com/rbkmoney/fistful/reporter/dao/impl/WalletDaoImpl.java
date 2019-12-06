package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.domain.tables.records.WalletRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.rbkmoney.fistful.reporter.domain.tables.Wallet.WALLET;

@Component
public class WalletDaoImpl extends AbstractGenericDao implements WalletDao {

    private final RowMapper<Wallet> walletRowMapper;

    @Autowired
    public WalletDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        walletRowMapper = new RecordRowMapper<>(WALLET, Wallet.class);
    }

    @Override
    public Optional<Long> getLastEventId() {
        Query query = getDslContext().select(DSL.max(WALLET.EVENT_ID)).from(WALLET);

        return Optional.ofNullable(fetchOne(query, Long.class));
    }

    @Override
    public Long save(Wallet wallet) {
        WalletRecord record = getDslContext().newRecord(WALLET, wallet);
        Query query = getDslContext().insertInto(WALLET).set(record).returning(WALLET.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Wallet get(String walletId) {
        Condition condition = WALLET.WALLET_ID.eq(walletId)
                .and(WALLET.CURRENT);
        Query query = getDslContext().selectFrom(WALLET).where(condition);

        return fetchOne(query, walletRowMapper);
    }

    @Override
    public void updateNotCurrent(String walletId) {
        Condition condition = WALLET.WALLET_ID.eq(walletId)
                .and(WALLET.CURRENT);
        Query query = getDslContext().update(WALLET).set(WALLET.CURRENT, false).where(condition);

        execute(query);
    }

}
