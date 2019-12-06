package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.domain.tables.records.DestinationRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import java.util.Optional;

import static com.rbkmoney.fistful.reporter.domain.tables.Destination.DESTINATION;

@Component
public class DestinationDaoImpl extends AbstractGenericDao implements DestinationDao {

    private final RowMapper<Destination> destinationRowMapper;

    @Autowired
    public DestinationDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        destinationRowMapper = new RecordRowMapper<>(DESTINATION, Destination.class);
    }

    @Override
    public Optional<Long> getLastEventId() {
        Query query = getDslContext().select(DSL.max(DESTINATION.EVENT_ID)).from(DESTINATION);

        return Optional.ofNullable(fetchOne(query, Long.class));
    }

    @Override
    public Long save(Destination destination) {
        DestinationRecord record = getDslContext().newRecord(DESTINATION, destination);
        Query query = getDslContext().insertInto(DESTINATION).set(record).returning(DESTINATION.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Destination get(String destinationId) {
        Condition condition = DESTINATION.DESTINATION_ID.eq(destinationId)
                .and(DESTINATION.CURRENT);
        Query query = getDslContext().selectFrom(DESTINATION).where(condition);

        return fetchOne(query, destinationRowMapper);
    }

    @Override
    public void updateNotCurrent(String destinationId) {
        Condition condition = DESTINATION.DESTINATION_ID.eq(destinationId)
                .and(DESTINATION.CURRENT);
        Query query = getDslContext().update(DESTINATION).set(DESTINATION.CURRENT, false).where(condition);

        execute(query);
    }
}
