package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.fistful.reporter.dao.SourceDao;
import com.rbkmoney.fistful.reporter.domain.enums.SourceEventType;
import com.rbkmoney.fistful.reporter.domain.enums.SourceStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Source;
import com.rbkmoney.fistful.reporter.exception.DaoException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.SourceEventHandler;
import com.rbkmoney.fistful.source.Change;
import com.rbkmoney.fistful.source.SinkEvent;
import com.rbkmoney.fistful.source.Status;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class SourceStatusChangedHandler implements SourceEventHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetStatus() && change.getStatus().isSetStatus();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            Status status = change.getStatus().getStatus();

            log.info("Start source status changed handling, eventId={}, sourceId={}, status={}", event.getId(), event.getSource(), status);

            Source source = sourceDao.get(event.getSource());

            source.setId(null);
            source.setWtime(null);

            source.setEventId(event.getId());
            source.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            source.setSourceId(event.getSource());
            source.setSequenceId(event.getPayload().getSequence());
            source.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            source.setEventType(SourceEventType.SOURCE_STATUS_CHANGED);
            source.setSourceStatus(TBaseUtil.unionFieldToEnum(status, SourceStatus.class));

            sourceDao.updateNotCurrent(event.getSource());
            sourceDao.save(source);
            log.info("Source status have been changed, eventId={}, sourceId={}, status={}", event.getId(), event.getSource(), status);
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
