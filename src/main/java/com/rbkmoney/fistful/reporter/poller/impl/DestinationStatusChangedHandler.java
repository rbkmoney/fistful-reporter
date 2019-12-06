package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.destination.Change;
import com.rbkmoney.fistful.destination.SinkEvent;
import com.rbkmoney.fistful.destination.Status;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationEventType;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.DestinationEventHandler;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DestinationStatusChangedHandler implements DestinationEventHandler {

    private final DestinationDao destinationDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetStatus() && change.getStatus().isSetChanged();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            Status status = change.getStatus().getChanged();
            log.info("Start destination status changed handling, eventId={}, destinationId={}, status={}", event.getId(), event.getSource(), status);

            Destination destination = destinationDao.get(event.getSource());

            destination.setId(null);
            destination.setWtime(null);

            destination.setEventId(event.getId());
            destination.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            destination.setDestinationId(event.getSource());
            destination.setSequenceId(event.getPayload().getSequence());
            destination.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            destination.setEventType(DestinationEventType.DESTINATION_STATUS_CHANGED);
            destination.setDestinationStatus(TBaseUtil.unionFieldToEnum(status, DestinationStatus.class));

            destinationDao.updateNotCurrent(event.getSource());
            destinationDao.save(destination);
            log.info("Destination status have been changed, eventId={}, destinationId={}, status={}", event.getId(), event.getSource(), status);
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
