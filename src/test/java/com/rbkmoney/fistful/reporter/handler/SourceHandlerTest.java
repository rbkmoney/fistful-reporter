package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.dao.SourceDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Source;
import com.rbkmoney.fistful.reporter.handler.source.SourceStatusChangedHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Source.SOURCE;
import static com.rbkmoney.fistful.reporter.util.handler.SourceHandlerTestUtil.createMachineEvent;
import static com.rbkmoney.fistful.reporter.util.handler.SourceHandlerTestUtil.createStatusChanged;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class SourceHandlerTest {

    @Autowired
    private SourceStatusChangedHandler sourceStatusChangedHandler;

    @Autowired
    private SourceDao sourceDao;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    Source source = random(Source.class);
    Account account = random(Account.class);
    String sqlStatement = "select * from fr.source where id='" + source.getId() + "';";

    @Test
    public void sourceAccountCreatedHandlerTest() {
        source.setCurrent(true);
        sourceDao.save(source);
        sourceStatusChangedHandler.handle(createStatusChanged(account),
                createMachineEvent(source.getSourceId(), account));
        assertEquals(2L, sourceDao.get(source.getSourceId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(SOURCE, Source.class)).getCurrent()
        );
    }
}
