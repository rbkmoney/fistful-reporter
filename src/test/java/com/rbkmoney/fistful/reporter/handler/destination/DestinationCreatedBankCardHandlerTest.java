package com.rbkmoney.fistful.reporter.handler.destination;

import com.rbkmoney.fistful.reporter.config.AbstractHandlerConfig;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.utils.handler.DestinationHandlerTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Destination.DESTINATION;
import static com.rbkmoney.fistful.reporter.utils.handler.DestinationHandlerTestUtils.createCreated;
import static com.rbkmoney.fistful.reporter.utils.handler.DestinationHandlerTestUtils.createCreatedMachineEvent;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class DestinationCreatedBankCardHandlerTest extends AbstractHandlerConfig {

    @Autowired
    private DestinationCreatedHandler destinationCreatedHandler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    Destination destination = random(Destination.class);
    String sqlStatement = "select * from fr.destination LIMIT 1;";

    @Before
    public void setUp() {
        destination.setCurrent(true);
    }

    @Test
    public void destinationCreatedHandlerTest() {
        com.rbkmoney.fistful.base.Resource fistfulResource = new com.rbkmoney.fistful.base.Resource();
        fistfulResource.setBankCard(DestinationHandlerTestUtils.createResourceBankCard());
        com.rbkmoney.fistful.destination.Destination fistfulDestination
                = DestinationHandlerTestUtils.createFistfulDestination(fistfulResource);

        destinationCreatedHandler.handle(
                createCreated(fistfulDestination),
                createCreatedMachineEvent(destination.getDestinationId(), fistfulDestination));

        Destination destinationResult = jdbcTemplate.queryForObject(sqlStatement,
                new RecordRowMapper<>(DESTINATION, Destination.class));
        assertEquals(true, destinationResult.getCurrent());

        Assert.assertNotNull(destinationResult.getResourceBankCardBin());
        Assert.assertNotNull(destinationResult.getResourceBankCardMaskedPan());
        Assert.assertNotNull(destinationResult.getResourceBankCardToken());
    }

    @Override
    protected int getExpectedSize() {
        return 0;
    }
}