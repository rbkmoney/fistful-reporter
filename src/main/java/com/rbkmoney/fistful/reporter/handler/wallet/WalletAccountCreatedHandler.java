package com.rbkmoney.fistful.reporter.handler.wallet;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.domain.enums.WalletEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.wallet.TimestampedChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletAccountCreatedHandler implements WalletEventHandler {

    private final IdentityDao identityDao;
    private final WalletDao walletDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetAccount() && change.getChange().getAccount().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Account account = change.getChange().getAccount().getCreated();
            log.info("Start wallet account created handling, eventId={}, walletId={}, identityId={}",
                    event.getEventId(), event.getSourceId(), account.getIdentity());

            Wallet wallet = getWallet(event);

            Long oldId = wallet.getId();

            wallet.setId(null);
            wallet.setWtime(null);
            wallet.setEventId(event.getEventId());
            wallet.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            wallet.setWalletId(event.getSourceId());
            wallet.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            wallet.setEventType(WalletEventType.WALLET_ACCOUNT_CREATED);
            wallet.setAccountId(account.getId());
            wallet.setAccounterAccountId(account.getAccounterAccountId());
            wallet.setCurrencyCode(account.getCurrency().getSymbolicCode());

            Identity identity = getIdentity(event, account);
            wallet.setPartyId(identity.getPartyId());
            wallet.setPartyContractId(identity.getPartyContractId());
            wallet.setIdentityId(identity.getIdentityId());

            walletDao.save(wallet).ifPresentOrElse(
                    id -> {
                        walletDao.updateNotCurrent(oldId);
                        log.info("Wallet account have been changed, eventId={}, walletId={}, identityId={}",
                                event.getEventId(), event.getSourceId(), account.getIdentity());
                    },
                    () -> log.info("Wallet account bound duplicated, eventId={}, walletId={}, identityId={}",
                            event.getEventId(), event.getSourceId(), account.getIdentity()));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Identity getIdentity(MachineEvent event, Account account) {
        Identity identity = identityDao.get(account.getIdentity());
        if (identity == null) {
            throw new SinkEventNotFoundException(
                    String.format("Identity not found, walletId='%s', identityId='%s'",
                            event.getSourceId(), account.getIdentity()));
        }
        return identity;
    }

    private Wallet getWallet(MachineEvent event) {
        Wallet wallet = walletDao.get(event.getSourceId());
        if (wallet == null) {
            throw new SinkEventNotFoundException(
                    String.format("Wallet not found, walletId='%s'", event.getSourceId()));
        }
        return wallet;
    }
}
