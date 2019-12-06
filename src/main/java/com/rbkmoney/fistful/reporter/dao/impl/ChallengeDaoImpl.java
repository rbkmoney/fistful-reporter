package com.rbkmoney.fistful.reporter.dao.impl;

import com.rbkmoney.dao.impl.AbstractGenericDao;
import com.rbkmoney.fistful.reporter.dao.ChallengeDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Challenge;
import com.rbkmoney.fistful.reporter.domain.tables.records.ChallengeRecord;
import com.zaxxer.hikari.HikariDataSource;
import org.jooq.Condition;
import org.jooq.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Component;

import static com.rbkmoney.fistful.reporter.domain.tables.Challenge.CHALLENGE;

@Component
public class ChallengeDaoImpl extends AbstractGenericDao implements ChallengeDao {

    private final RowMapper<Challenge> challengeRowMapper;

    @Autowired
    public ChallengeDaoImpl(HikariDataSource dataSource) {
        super(dataSource);
        challengeRowMapper = new RecordRowMapper<>(CHALLENGE, Challenge.class);
    }

    @Override
    public Long save(Challenge challenge) {
        ChallengeRecord record = getDslContext().newRecord(CHALLENGE, challenge);
        Query query = getDslContext().insertInto(CHALLENGE).set(record).returning(CHALLENGE.ID);

        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        executeOne(query, keyHolder);
        return keyHolder.getKey().longValue();
    }

    @Override
    public Challenge get(String identityId, String challengeId) {
        Condition condition = CHALLENGE.IDENTITY_ID.eq(identityId)
                .and(CHALLENGE.CHALLENGE_ID.eq(challengeId))
                .and(CHALLENGE.CURRENT);
        Query query = getDslContext().selectFrom(CHALLENGE).where(condition);

        return fetchOne(query, challengeRowMapper);
    }

    @Override
    public void updateNotCurrent(String identityId, String challengeId) {
        Condition condition = CHALLENGE.IDENTITY_ID.eq(identityId)
                .and(CHALLENGE.CHALLENGE_ID.eq(challengeId))
                .and(CHALLENGE.CURRENT);
        Query query = getDslContext().update(CHALLENGE).set(CHALLENGE.CURRENT, false).where(condition);

        execute(query);
    }
}
