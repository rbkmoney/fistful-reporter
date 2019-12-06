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
public class IdentityCreatedHandler implements IdentityEventHandler {

    private final IdentityDao identityDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            log.info("Start identity created handling, eventId={}, identityId={}", event.getId(), event.getSource());
            Identity identity = new Identity();

            identity.setEventId(event.getId());
            identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            identity.setIdentityId(event.getSource());
            identity.setSequenceId(event.getPayload().getSequence());
            identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            identity.setEventType(IdentityEventType.IDENTITY_CREATED);
            identity.setPartyId(change.getCreated().getParty());
            identity.setIdentityProviderId(change.getCreated().getProvider());
            identity.setIdentityClassId(change.getCreated().getCls());
            identity.setPartyContractId(change.getCreated().getContract());

            identityDao.updateNotCurrent(event.getSource());
            identityDao.save(identity);
            log.info("Identity haven been saved, eventId={}, identityId={}", event.getId(), event.getSource());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
