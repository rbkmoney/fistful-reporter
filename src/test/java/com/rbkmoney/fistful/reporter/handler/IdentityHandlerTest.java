package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.config.AbstractHandlerConfig;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.handler.identity.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Identity.IDENTITY;
import static com.rbkmoney.fistful.reporter.utils.handler.IdentityHandlerTestUtils.*;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class IdentityHandlerTest extends AbstractHandlerConfig {

    @Autowired
    private IdentityLevelChangedHandler identityLevelChangedHandler;

    @Autowired
    private IdentityEffectiveChallengeChangedHandler identityEffectiveChallengeChangedHandler;

    @Autowired
    private IdentityChallengeCreatedHandler identityChallengeCreatedHandler;


    @Autowired
    private JdbcTemplate jdbcTemplate;

    Identity identity = random(Identity.class);
    String sqlStatement = "select * from fr.identity where id='" + identity.getId() + "';";

    @Before
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



    @Override
    protected int getExpectedSize() {
        return 0;
    }
}
