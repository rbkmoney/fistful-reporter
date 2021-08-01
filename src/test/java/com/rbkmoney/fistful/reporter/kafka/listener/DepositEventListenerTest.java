package com.rbkmoney.fistful.reporter.kafka.listener;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.deposit.Change;
import com.rbkmoney.fistful.deposit.StatusChange;
import com.rbkmoney.fistful.deposit.TimestampedChange;
import com.rbkmoney.fistful.deposit.status.Status;
import com.rbkmoney.fistful.deposit.status.Succeeded;
import com.rbkmoney.fistful.reporter.config.KafkaPostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.domain.enums.DepositStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Deposit;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.testcontainers.annotations.kafka.config.KafkaProducer;
import org.apache.thrift.TBase;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;

import static com.rbkmoney.fistful.reporter.data.TestData.machineEvent;
import static com.rbkmoney.fistful.reporter.data.TestData.sinkEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@KafkaPostgresqlSpringBootITest
public class DepositEventListenerTest {

    @Value("${kafka.topic.deposit.name}")
    private String topicName;

    @Autowired
    private KafkaProducer<TBase<?, ?>> testThriftKafkaProducer;

    @MockBean
    private DepositDao depositDao;

    @Captor
    private ArgumentCaptor<Deposit> captor;

    @Test
    public void shouldListenAndSave() throws InterruptedException, DaoException {
        // Given
        TimestampedChange statusChanged = new TimestampedChange()
                .setOccuredAt("2016-03-22T06:12:27Z")
                .setChange(Change.status_changed(
                        new StatusChange().setStatus(
                                Status.succeeded(new Succeeded()))));

        SinkEvent sinkEvent = sinkEvent(
                machineEvent(
                        new ThriftSerializer<>(),
                        statusChanged));

        when(depositDao.get("source_id"))
                .thenReturn(new Deposit());

        // When
        testThriftKafkaProducer.send(topicName, sinkEvent);

        // Then
        verify(depositDao, timeout(5000).times(1))
                .save(captor.capture());
        assertThat(captor.getValue().getDepositStatus())
                .isEqualTo(DepositStatus.succeeded);
    }
}
