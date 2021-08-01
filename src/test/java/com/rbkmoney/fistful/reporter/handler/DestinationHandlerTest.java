package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.handler.destination.DestinationStatusChangedHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Destination.DESTINATION;
import static com.rbkmoney.fistful.reporter.util.handler.DestinationHandlerTestUtil.createMachineEvent;
import static com.rbkmoney.fistful.reporter.util.handler.DestinationHandlerTestUtil.createStatusChanged;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class DestinationHandlerTest {

    @Autowired
    private DestinationStatusChangedHandler destinationStatusChangedHandler;

    @Autowired
    private DestinationDao destinationDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    Destination destination = random(Destination.class);
    String sqlStatement = "select * from fr.destination where id='" + destination.getId() + "';";

    @BeforeEach
    public void setUp() {
        destination.setCurrent(true);
        destinationDao.save(destination);
    }

    @Test
    public void destinationStatusChangedHandlerTest() {
        destinationStatusChangedHandler.handle(
                createStatusChanged(),
                createMachineEvent(destination.getDestinationId()));
        assertEquals(2L, destinationDao.get(destination.getDestinationId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(DESTINATION, Destination.class)).getCurrent()
        );
    }
}
