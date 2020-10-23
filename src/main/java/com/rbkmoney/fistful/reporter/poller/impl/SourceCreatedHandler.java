package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.SourceDao;
import com.rbkmoney.fistful.reporter.domain.enums.SourceEventType;
import com.rbkmoney.fistful.reporter.domain.enums.SourceStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Source;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.SourceEventHandler;
import com.rbkmoney.fistful.source.Change;
import com.rbkmoney.fistful.source.Internal;
import com.rbkmoney.fistful.source.Resource;
import com.rbkmoney.fistful.source.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class SourceCreatedHandler implements SourceEventHandler {

    private final SourceDao sourceDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            log.info("Start source created handling, eventId={}, sourceId={}", event.getId(), event.getSource());
            Source source = new Source();

            source.setEventId(event.getId());
            source.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            source.setSourceId(event.getSource());
            source.setSequenceId(event.getPayload().getSequence());
            source.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            source.setEventType(SourceEventType.SOURCE_CREATED);
            source.setSourceName(change.getCreated().getName());
            source.setSourceStatus(SourceStatus.unauthorized);

            Resource resource = change.getCreated().getResource();
            if (resource.isSetInternal()) {
                Internal internal = resource.getInternal();
                source.setResourceInternalDetails(internal.getDetails());
            }

            sourceDao.updateNotCurrent(event.getSource());
            sourceDao.save(source);
            log.info("Source has been saved, eventId={}, sourceId={}", event.getId(), event.getSource());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
