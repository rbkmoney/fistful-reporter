package com.rbkmoney.fistful.reporter.handler.source;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.SourceDao;
import com.rbkmoney.fistful.reporter.domain.enums.SourceEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Source;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.source.TimestampedChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class SourceAccountCreatedHandler implements SourceEventHandler {

    private final SourceDao sourceDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAccount() && change.getChange().getAccount().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Account account = change.getChange().getAccount().getCreated();
            log.info("Start source account created handling, eventId={}, sourceId={}, identityId={}",
                    event.getEventId(), event.getSourceId(), account.getIdentity());
            Source source = getSource(event);

            Long oldId = source.getId();

            source.setId(null);
            source.setWtime(null);
            source.setEventId(event.getEventId());
            source.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            source.setSourceId(event.getSourceId());
            source.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            source.setEventType(SourceEventType.SOURCE_ACCOUNT_CREATED);
            source.setAccountId(account.getId());
            source.setAccounterAccountId(account.getAccounterAccountId());
            source.setCurrencyCode(account.getCurrency().getSymbolicCode());

            Identity identity = getIdentity(event, account);
            source.setPartyId(identity.getPartyId());
            source.setPartyContractId(identity.getPartyContractId());
            source.setIdentityId(identity.getIdentityId());

            sourceDao.save(source).ifPresentOrElse(
                    id -> {
                        sourceDao.updateNotCurrent(oldId);
                        log.info("Source account have been changed, eventId={}, sourceId={}, identityId={}",
                                event.getEventId(), event.getSourceId(), account.getIdentity());
                    },
                    () -> log.info("Source account bound duplicated, eventId={}, sourceId={}, identityId={}",
                            event.getEventId(), event.getSourceId(), account.getIdentity()));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Source getSource(MachineEvent event) {
        Source source = sourceDao.get(event.getSourceId());
        if (source == null) {
            throw new SinkEventNotFoundException(String.format("Source not found, sourceId='%s'", event.getSourceId()));
        }
        return source;
    }

    private Identity getIdentity(MachineEvent event, Account account) {
        Identity identity = identityDao.get(account.getIdentity());
        if (identity == null) {
            throw new SinkEventNotFoundException(
                    String.format("Identity not found, sourceId='%s', identityId='%s'",
                            event.getSourceId(), account.getIdentity()));
        }
        return identity;
    }
}
