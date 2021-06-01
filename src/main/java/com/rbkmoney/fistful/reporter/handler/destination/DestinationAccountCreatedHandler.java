package com.rbkmoney.fistful.reporter.handler.destination;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.destination.TimestampedChange;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DestinationAccountCreatedHandler implements DestinationEventHandler {

    private final DestinationDao destinationDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAccount() && change.getChange().getAccount().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start destination account created handling, eventId={}, destinationId={}",
                    event.getEventId(), event.getSourceId());
            Account account = change.getChange().getAccount().getCreated();
            Destination oldDestination = getDestination(event);
            Destination updatedDestination = update(oldDestination, change, event, account);
            destinationDao.save(updatedDestination).ifPresentOrElse(
                    id -> {
                        destinationDao.updateNotCurrent(oldDestination.getId());
                        log.info("Destination account have been created, eventId={}, destinationId={}, identityId={}",
                                event.getEventId(), event.getSourceId(), account.getIdentity());
                    },
                    () -> log.info("Destination account create bound duplicated, " +
                                    "eventId={}, destinationId={}, identityId={}",
                            event.getEventId(), event.getSourceId(), account.getIdentity()));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Destination update(
            Destination oldDestination,
            TimestampedChange change,
            MachineEvent event,
            Account account) {
        Destination destination = new Destination(oldDestination);
        destination.setId(null);
        destination.setWtime(null);
        destination.setEventId(event.getEventId());
        destination.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        destination.setDestinationId(event.getSourceId());
        destination.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        destination.setEventType(DestinationEventType.DESTINATION_ACCOUNT_CREATED);

        destination.setAccountId(account.getId());
        destination.setCurrencyCode(account.getCurrency().getSymbolicCode());
        destination.setAccounterAccountId(account.getAccounterAccountId());

        Identity identity = getIdentity(event, account);
        destination.setPartyId(identity.getPartyId());
        destination.setPartyContractId(identity.getPartyContractId());
        destination.setIdentityId(identity.getIdentityId());
        return destination;
    }

    private Destination getDestination(MachineEvent event) {
        Destination destination = destinationDao.get(event.getSourceId());
        if (destination == null) {
            throw new SinkEventNotFoundException(
                    String.format("Destination not found, destinationId='%s'", event.getSourceId()));
        }
        return destination;
    }

    private Identity getIdentity(MachineEvent event, Account account) {
        Identity identity = identityDao.get(account.getIdentity());
        if (identity == null) {
            throw new SinkEventNotFoundException(
                    String.format("Identity not found, destinationId='%s', identityId='%s'",
                            event.getSourceId(), account.getIdentity()));
        }
        return identity;
    }
}
