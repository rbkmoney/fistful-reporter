package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.destination.Change;
import com.rbkmoney.fistful.destination.SinkEvent;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.DestinationEventHandler;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DestinationAccountCreatedHandler implements DestinationEventHandler {

    private final DestinationDao destinationDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetAccount() && change.getAccount().isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            log.info("Start destination account created handling, eventId={}, destinationId={}", event.getId(), event.getSource());
            Account account = change.getAccount().getCreated();
            Destination destination = getDestination(event);

            Identity identity = getIdentity(event, account);

            destination.setId(null);
            destination.setWtime(null);

            destination.setEventId(event.getId());
            destination.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            destination.setDestinationId(event.getSource());
            destination.setSequenceId(event.getPayload().getSequence());
            destination.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            destination.setEventType(DestinationEventType.DESTINATION_ACCOUNT_CREATED);
            destination.setAccountId(account.getId());
            destination.setCurrencyCode(account.getCurrency().getSymbolicCode());
            destination.setAccounterAccountId(account.getAccounterAccountId());

            destination.setPartyId(identity.getPartyId());
            destination.setPartyContractId(identity.getPartyContractId());
            destination.setIdentityId(identity.getIdentityId());

            destinationDao.updateNotCurrent(event.getSource());
            destinationDao.save(destination);
            log.info("Destination account have been saved, eventId={}, destinationId={}, identityId={}", event.getId(), event.getSource(), account.getIdentity());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Destination getDestination(SinkEvent event) {
        Destination destination = destinationDao.get(event.getSource());
        if (destination == null) {
            throw new SinkEventNotFoundException(String.format("Destination not found, destinationId='%s'", event.getSource()));
        }
        return destination;
    }

    private Identity getIdentity(SinkEvent event, Account account) {
        Identity identity = identityDao.get(account.getIdentity());
        if (identity == null) {
            throw new SinkEventNotFoundException(String.format("Identity not found, destinationId='%s', identityId='%s'", event.getSource(), account.getIdentity()));
        }
        return identity;
    }
}
