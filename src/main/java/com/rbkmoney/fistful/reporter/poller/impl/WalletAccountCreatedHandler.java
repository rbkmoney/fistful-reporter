package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.domain.enums.WalletEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.WalletEventHandler;
import com.rbkmoney.fistful.wallet.Change;
import com.rbkmoney.fistful.wallet.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class WalletAccountCreatedHandler implements WalletEventHandler {

    private final IdentityDao identityDao;
    private final WalletDao walletDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetAccount() && change.getAccount().isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            Account account = change.getAccount().getCreated();
            log.info("Start wallet account created handling, eventId={}, walletId={}, identityId={}", event.getId(), event.getSource(), account.getIdentity());
            Wallet wallet = getWallet(event);
            Identity identity = getIdentity(event, account);

            wallet.setId(null);
            wallet.setWtime(null);

            wallet.setEventId(event.getId());
            wallet.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            wallet.setWalletId(event.getSource());
            wallet.setSequenceId(event.getPayload().getSequence());
            wallet.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            wallet.setEventType(WalletEventType.WALLET_ACCOUNT_CREATED);
            wallet.setAccountId(account.getId());
            wallet.setAccounterAccountId(account.getAccounterAccountId());
            wallet.setCurrencyCode(account.getCurrency().getSymbolicCode());

            wallet.setPartyId(identity.getPartyId());
            wallet.setPartyContractId(identity.getPartyContractId());
            wallet.setIdentityId(identity.getIdentityId());

            walletDao.updateNotCurrent(event.getSource());
            walletDao.save(wallet);
            log.info("Wallet account has been saved, eventId={}, walletId={}, identityId={}", event.getId(), event.getSource(), account.getIdentity());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Identity getIdentity(SinkEvent event, Account account) {
        Identity identity = identityDao.get(account.getIdentity());
        if (identity == null) {
            throw new SinkEventNotFoundException(String.format("Identity not found, walletId=%s, identityId=%s", event.getSource(), account.getIdentity()));
        }
        return identity;
    }

    private Wallet getWallet(SinkEvent event) {
        Wallet wallet = walletDao.get(event.getSource());
        if (wallet == null) {
            throw new SinkEventNotFoundException(String.format("Wallet not found, walletId=%s", event.getSource()));
        }
        return wallet;
    }
}
