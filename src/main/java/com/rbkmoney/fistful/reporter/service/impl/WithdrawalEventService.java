package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.WithdrawalEventHandler;
import com.rbkmoney.fistful.reporter.service.EventService;
import com.rbkmoney.fistful.withdrawal.Change;
import com.rbkmoney.fistful.withdrawal.SinkEvent;
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
public class WithdrawalEventService implements EventService<SinkEvent> {

    private final List<WithdrawalEventHandler> eventHandlers;
    private final WithdrawalDao withdrawalDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Optional<Long> getLastEventId() {
        try {
            Optional<Long> lastEventId = withdrawalDao.getLastEventId();
            log.info("Last withdrawal eventId = {}", lastEventId);
            return lastEventId;
        } catch (DaoException e) {
            throw new StorageException("Failed to get last withdrawal event id", e);
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void processSinkEvent(SinkEvent event) {
        for (Change change : event.getPayload().getChanges()) {
            for (WithdrawalEventHandler identityEventHandler : eventHandlers) {
                if (identityEventHandler.accept(change)) {
                    identityEventHandler.handle(change, event);
                }
            }
        }
    }
}
