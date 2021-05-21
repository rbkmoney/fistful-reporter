package com.rbkmoney.fistful.reporter.handler.identity;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.identity.TimestampedChange;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.IdentityEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityCreatedHandler implements IdentityEventHandler {

    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start identity created handling, eventId={}, identityId={}",
                    event.getEventId(), event.getSourceId());
            Identity identity = new Identity();
            identity.setEventId(event.getEventId());
            identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            identity.setIdentityId(event.getSourceId());
            identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            identity.setEventType(IdentityEventType.IDENTITY_CREATED);
            identity.setPartyId(change.getChange().getCreated().getParty());
            identity.setIdentityProviderId(change.getChange().getCreated().getProvider());
            identity.setIdentityClassId(change.getChange().getCreated().getCls());
            identity.setPartyContractId(change.getChange().getCreated().getContract());

            identityDao.save(identity).ifPresentOrElse(
                    id -> log.info("Identity haven been saved, eventId={}, identityId={}",
                            event.getEventId(), event.getSourceId()),
                    () -> log.info("Identity created bound duplicated, eventId={}, identityId={}",
                            event.getEventId(), event.getSourceId())
            );
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
