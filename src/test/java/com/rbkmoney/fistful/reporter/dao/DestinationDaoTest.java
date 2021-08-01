package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@PostgresqlSpringBootITest
public class DestinationDaoTest {

    @Autowired
    private DestinationDao destinationDao;

    @Test
    public void destinationDaoTest() {
        Destination destination = random(Destination.class);
        destination.setCurrent(true);
        Long id = destinationDao.save(destination).get();
        destination.setId(id);
        assertEquals(destination, destinationDao.get(destination.getDestinationId()));
        destinationDao.updateNotCurrent(destination.getId());
        assertNull(destinationDao.get(destination.getDestinationId()));
    }
}
