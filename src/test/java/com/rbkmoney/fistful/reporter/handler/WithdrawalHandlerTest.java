package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.config.AbstractHandlerConfig;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.handler.withdrawal.*;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Withdrawal.WITHDRAWAL;
import static com.rbkmoney.fistful.reporter.utils.handler.WithdrawalHandlerTestUtils.*;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class WithdrawalHandlerTest extends AbstractHandlerConfig {

    @Autowired
    private WithdrawalStatusChangedHandler withdrawalStatusChangedHandler;

    @Autowired
    private WithdrawalTransferStatusChangedHandler withdrawalTransferStatusChangedHandler;

    @Autowired
    private WithdrawalTransferCreatedHandler withdrawalTransferCreatedHandler;

    @Autowired
    private WithdrawalRouteChangeHandler withdrawalRouteChangeHandler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    Withdrawal withdrawal = random(Withdrawal.class);
    String sqlStatement = "select * from fr.withdrawal where id='" + withdrawal.getId() + "';";

    @Before
    public void setUp() {
        withdrawal.setCurrent(true);
        withdrawalDao.save(withdrawal);
    }

    @Test
    public void withdrawalStatusChangedHandlerTest() {
        withdrawalStatusChangedHandler.handle(createStatusChanged(), createMachineEvent(withdrawal.getWithdrawalId()));
        assertEquals(2L, withdrawalDao.get(withdrawal.getWithdrawalId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(WITHDRAWAL, Withdrawal.class)).getCurrent()
        );
    }

    @Test
    public void withdrawalTransferStatusChangedHandlerTest() {
        withdrawalTransferStatusChangedHandler.handle(
                createTransferChanged(),
                createMachineEvent(withdrawal.getWithdrawalId()));
        assertEquals(2L, withdrawalDao.get(withdrawal.getWithdrawalId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(WITHDRAWAL, Withdrawal.class)).getCurrent()
        );
    }

    @Test
    public void withdrawalTransferCreatedHandlerTest() {
        withdrawalTransferCreatedHandler.handle(
                createTransferCreated(),
                createMachineEvent(withdrawal.getWithdrawalId()));
        assertEquals(2L, withdrawalDao.get(withdrawal.getWithdrawalId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(WITHDRAWAL, Withdrawal.class)).getCurrent()
        );
    }

    @Test
    public void withdrawalRouteChangeHandlerTest() {
        withdrawalRouteChangeHandler.handle(createTransferCreated(), createMachineEvent(withdrawal.getWithdrawalId()));
        assertEquals(2L, withdrawalDao.get(withdrawal.getWithdrawalId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(
                        sqlStatement, new RecordRowMapper<>(WITHDRAWAL, Withdrawal.class)).getCurrent()
        );
    }


    @Override
    protected int getExpectedSize() {
        return 0;
    }
}
