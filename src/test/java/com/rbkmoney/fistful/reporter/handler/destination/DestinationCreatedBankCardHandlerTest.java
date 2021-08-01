package com.rbkmoney.fistful.reporter.handler.destination;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.util.handler.DestinationHandlerTestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Destination.DESTINATION;
import static com.rbkmoney.fistful.reporter.util.handler.DestinationHandlerTestUtil.createCreated;
import static com.rbkmoney.fistful.reporter.util.handler.DestinationHandlerTestUtil.createCreatedMachineEvent;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@PostgresqlSpringBootITest
public class DestinationCreatedBankCardHandlerTest {

    @Autowired
    private DestinationCreatedHandler destinationCreatedHandler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    public void destinationCreatedHandlerTest() {
        Destination destination = random(Destination.class);
        destination.setCurrent(true);
        String sqlStatement = "select * from fr.destination LIMIT 1;";
        com.rbkmoney.fistful.base.Resource fistfulResource = new com.rbkmoney.fistful.base.Resource();
        fistfulResource.setBankCard(DestinationHandlerTestUtil.createResourceBankCard());
        com.rbkmoney.fistful.destination.Destination fistfulDestination
                = DestinationHandlerTestUtil.createFistfulDestination(fistfulResource);

        destinationCreatedHandler.handle(
                createCreated(fistfulDestination),
                createCreatedMachineEvent(destination.getDestinationId(), fistfulDestination));

        Destination destinationResult = jdbcTemplate.queryForObject(sqlStatement,
                new RecordRowMapper<>(DESTINATION, Destination.class));
        assertEquals(true, destinationResult.getCurrent());

        assertNotNull(destinationResult.getResourceBankCardBin());
        assertNotNull(destinationResult.getResourceBankCardMaskedPan());
        assertNotNull(destinationResult.getResourceBankCardToken());
    }
}
