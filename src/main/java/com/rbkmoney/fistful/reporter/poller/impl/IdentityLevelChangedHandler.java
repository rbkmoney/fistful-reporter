package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.identity.Change;
import com.rbkmoney.fistful.identity.SinkEvent;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.IdentityEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.IdentityEventHandler;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class IdentityLevelChangedHandler implements IdentityEventHandler {

    private final IdentityDao identityDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetLevelChanged();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            log.info("Start identity level changed handling, eventId={}, identityId={}, level={}", event.getId(), event.getSource(), change.getLevelChanged());
            Identity identity = identityDao.get(event.getSource());

            identity.setId(null);
            identity.setWtime(null);

            identity.setEventId(event.getId());
            identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            identity.setIdentityId(event.getSource());
            identity.setSequenceId(event.getPayload().getSequence());
            identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            identity.setEventType(IdentityEventType.IDENTITY_LEVEL_CHANGED);
            identity.setIdentityLevelId(change.getLevelChanged());

            identityDao.updateNotCurrent(event.getSource());
            identityDao.save(identity);
            log.info("Identity level has been changed, eventId={}, identityId={}, level={}", event.getId(), event.getSource(), change.getLevelChanged());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

}
