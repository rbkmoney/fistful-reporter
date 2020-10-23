package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.SourceDao;
import com.rbkmoney.fistful.reporter.domain.enums.SourceEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Source;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.SourceEventHandler;
import com.rbkmoney.fistful.source.Change;
import com.rbkmoney.fistful.source.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class SourceAccountCreatedHandler implements SourceEventHandler {

    private final SourceDao sourceDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetAccount() && change.getAccount().isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            Account account = change.getAccount().getCreated();
            log.info("Start source account created handling, eventId={}, sourceId={}, identityId={}", event.getId(), event.getSource(), account.getIdentity());
            Source source = getSource(event);
            Identity identity = getIdentity(event, account);

            source.setId(null);
            source.setWtime(null);

            source.setEventId(event.getId());
            source.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            source.setSourceId(event.getSource());
            source.setSequenceId(event.getPayload().getSequence());
            source.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            source.setEventType(SourceEventType.SOURCE_ACCOUNT_CREATED);
            source.setAccountId(account.getId());
            source.setAccounterAccountId(account.getAccounterAccountId());
            source.setCurrencyCode(account.getCurrency().getSymbolicCode());

            source.setPartyId(identity.getPartyId());
            source.setPartyContractId(identity.getPartyContractId());
            source.setIdentityId(identity.getIdentityId());

            sourceDao.updateNotCurrent(event.getSource());
            sourceDao.save(source);
            log.info("Source account has been saved, eventId={}, sourceId={}, identityId={}", event.getId(), event.getSource(), account.getIdentity());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Source getSource(SinkEvent event) {
        Source source = sourceDao.get(event.getSource());
        if (source == null) {
            throw new SinkEventNotFoundException(String.format("Source not found, sourceId=%s", event.getSource()));
        }
        return source;
    }

    private Identity getIdentity(SinkEvent event, Account account) {
        Identity identity = identityDao.get(account.getIdentity());
        if (identity == null) {
            throw new SinkEventNotFoundException(String.format("Identity not found, sourceId=%s, identityId=%s", event.getSource(), account.getIdentity()));
        }
        return identity;
    }
}
