package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.records.IdentityRecord;
import com.rbkmoney.fistful.reporter.exception.DaoException;
import org.jooq.Condition;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Optional;

import static com.rbkmoney.fistful.reporter.domain.tables.Challenge.CHALLENGE;
import static com.rbkmoney.fistful.reporter.domain.tables.Identity.IDENTITY;

@Component
public class IdentityDaoImpl extends AbstractGenericDao implements IdentityDao {

    private final RowMapper<Identity> identityRowMapper;

    @Autowired
    public IdentityDaoImpl(DataSource dataSource) {
        super(dataSource);
        identityRowMapper = new RecordRowMapper<>(IDENTITY, Identity.class);
    }

    @Override
    public Optional<Long> getLastEventId() throws DaoException {
        String eventId = "event_id";
        Query query = getDslContext().select(DSL.max(DSL.field(eventId)))
                .from(
                        getDslContext().select(DSL.max(IDENTITY.EVENT_ID).as(eventId))
                                .from(IDENTITY)
                                .unionAll(
                                        getDslContext().select(DSL.max(CHALLENGE.EVENT_ID).as(eventId))
                                                .from(CHALLENGE)
                                )
                );

        return Optional.ofNullable(fetchOne(query, Long.class));
    }

    @Override
    public Long save(Identity identity) throws DaoException {
        IdentityRecord record = getDslContext().newRecord(IDENTITY, identity);
        Query query = getDslContext().insertInto(IDENTITY).set(record).returning(IDENTITY.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Identity get(String identityId) throws DaoException {
        Condition condition = IDENTITY.IDENTITY_ID.eq(identityId)
                .and(IDENTITY.CURRENT);
        Query query = getDslContext().selectFrom(IDENTITY).where(condition);

        return fetchOne(query, identityRowMapper);
    }

    @Override
    public void updateNotCurrent(String identityId) throws DaoException {
        Condition condition = IDENTITY.IDENTITY_ID.eq(identityId)
                .and(IDENTITY.CURRENT);
        Query query = getDslContext().update(IDENTITY).set(IDENTITY.CURRENT, false).where(condition);

        executeOne(query);
    }
}