package com.rbkmoney.fistful.reporter.listener;

import com.rbkmoney.fistful.reporter.exception.NotFoundException;
import com.rbkmoney.fistful.reporter.service.impl.SourceEventService;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toList;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceEventListener {

    @Value("${kafka.retry-delay-ms}")
    private int retryDelayMs;

    private final SourceEventService sourceEventService;

    @KafkaListener(
            autoStartup = "${kafka.topic.source.listener.enabled}",
            topics = "${kafka.topic.source.name}",
            containerFactory = "sourceEventListenerContainerFactory")
    public void listen(
            List<SinkEvent> batch,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) int offset,
            Acknowledgment ack) throws InterruptedException {
        log.info("Listening Source: partition={}, offset={}, batch.size()={}", partition, offset, batch.size());
        try {
            sourceEventService.handleEvents(batch.stream().map(SinkEvent::getEvent).collect(toList()));
        } catch (
                NotFoundException e) {
            log.info("Delayed retry caused by an exception", e);
            TimeUnit.MILLISECONDS.sleep(retryDelayMs);
            throw e;
        }
        ack.acknowledge();
        log.info("Ack Source: partition={}, offset={}", partition, offset);
    }
}
