package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.records.IdentityRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.rbkmoney.fistful.reporter.domain.tables.Challenge.CHALLENGE;
import static com.rbkmoney.fistful.reporter.domain.tables.Identity.IDENTITY;

@Component
public class IdentityDaoImpl extends AbstractGenericDao implements IdentityDao {

    private static final String EVENT_ID = "event_id";

    private final RowMapper<Identity> identityRowMapper;

    @Autowired
    public IdentityDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        identityRowMapper = new RecordRowMapper<>(IDENTITY, Identity.class);
    }

    @Override
    public Optional<Long> getLastEventId() {
        Query query = getDslContext().select(DSL.max(DSL.field(EVENT_ID)))
                .from(
                        getDslContext().select(DSL.max(IDENTITY.EVENT_ID).as(EVENT_ID))
                                .from(IDENTITY)
                                .unionAll(
                                        getDslContext().select(DSL.max(CHALLENGE.EVENT_ID).as(EVENT_ID))
                                                .from(CHALLENGE)
                                )
                );

        return Optional.ofNullable(fetchOne(query, Long.class));
    }

    @Override
    public Long save(Identity identity) {
        IdentityRecord record = getDslContext().newRecord(IDENTITY, identity);
        Query query = getDslContext().insertInto(IDENTITY).set(record).returning(IDENTITY.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Identity get(String identityId) {
        Condition condition = IDENTITY.IDENTITY_ID.eq(identityId)
                .and(IDENTITY.CURRENT);
        Query query = getDslContext().selectFrom(IDENTITY).where(condition);

        return fetchOne(query, identityRowMapper);
    }

    @Override
    public void updateNotCurrent(String identityId) {
        Condition condition = IDENTITY.IDENTITY_ID.eq(identityId)
                .and(IDENTITY.CURRENT);
        Query query = getDslContext().update(IDENTITY).set(IDENTITY.CURRENT, false).where(condition);

        execute(query);
    }
}
