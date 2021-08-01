package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.handler.identity.IdentityChallengeCreatedHandler;
import com.rbkmoney.fistful.reporter.handler.identity.IdentityEffectiveChallengeChangedHandler;
import com.rbkmoney.fistful.reporter.handler.identity.IdentityLevelChangedHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Identity.IDENTITY;
import static com.rbkmoney.fistful.reporter.util.handler.IdentityHandlerTestUtil.*;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class IdentityHandlerTest {

    @Autowired
    private IdentityLevelChangedHandler identityLevelChangedHandler;

    @Autowired
    private IdentityEffectiveChallengeChangedHandler identityEffectiveChallengeChangedHandler;

    @Autowired
    private IdentityChallengeCreatedHandler identityChallengeCreatedHandler;

    @Autowired
    private IdentityDao identityDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    Identity identity = random(Identity.class);
    String sqlStatement = "select * from fr.identity where id='" + identity.getId() + "';";

    @BeforeEach
    public void setUp() {
        identity.setCurrent(true);
        identityDao.save(identity);
    }

    @Test
    public void identityLevelChangedHandlerTest() {
        identityLevelChangedHandler.handle(createLevelChanged(), createMachineEvent(identity.getIdentityId()));
        assertEquals(2L, identityDao.get(identity.getIdentityId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(IDENTITY, Identity.class)).getCurrent()
        );
    }

    @Test
    public void identityEffectiveChallengeChangedHandlerTest() {
        identityEffectiveChallengeChangedHandler.handle(
                createEffectiveChallengeChanged(),
                createMachineEvent(identity.getIdentityId()));
        assertEquals(2L, identityDao.get(identity.getIdentityId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(IDENTITY, Identity.class)).getCurrent()
        );
    }

    @Test
    public void identityChallengeCreatedHandlerTest() {
        identityChallengeCreatedHandler.handle(createChallengeCreated(identity.getIdentityId()),
                createMachineEvent(identity.getIdentityId()));
        assertEquals(2L, identityDao.get(identity.getIdentityId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(IDENTITY, Identity.class)).getCurrent()
        );
    }
}
