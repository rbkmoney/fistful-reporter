package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.SourceDao;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.SourceEventHandler;
import com.rbkmoney.fistful.reporter.service.EventService;
import com.rbkmoney.fistful.source.Change;
import com.rbkmoney.fistful.source.SinkEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class SourceEventService implements EventService<SinkEvent> {

    private final List<SourceEventHandler> eventHandlers;
    private final SourceDao sourceDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Optional<Long> getLastEventId() {
        try {
            Optional<Long> lastEventId = sourceDao.getLastEventId();
            log.info("Last source eventId = {}", lastEventId);
            return lastEventId;
        } catch (DaoException e) {
            throw new StorageException("Failed to get last source event id", e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void processSinkEvent(SinkEvent event) {
        for (Change change : event.getPayload().getChanges()) {
            for (SourceEventHandler identityEventHandler : eventHandlers) {
                if (identityEventHandler.accept(change)) {
                    identityEventHandler.handle(change, event);
                }
            }
        }
    }
}
