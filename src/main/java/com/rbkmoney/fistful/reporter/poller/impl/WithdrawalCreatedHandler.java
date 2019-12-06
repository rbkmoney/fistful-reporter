package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.base.Cash;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.WithdrawalEventHandler;
import com.rbkmoney.fistful.withdrawal.Change;
import com.rbkmoney.fistful.withdrawal.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class WithdrawalCreatedHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final WalletDao walletDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetCreated() && change.getCreated().isSetWithdrawal();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            var withdrawalDamsel = change.getCreated().getWithdrawal();

            log.info("Start withdrawal created handling, eventId={}, walletId={}", event.getId(), event.getSource());

            Wallet wallet = getWallet(event, withdrawalDamsel.getSource());

            Withdrawal withdrawal = new Withdrawal();
            withdrawal.setExternalId(withdrawalDamsel.getExternalId());
            withdrawal.setEventId(event.getId());
            withdrawal.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            withdrawal.setWithdrawalId(event.getSource());
            withdrawal.setSequenceId(event.getPayload().getSequence());
            withdrawal.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_CREATED);
            withdrawal.setWalletId(withdrawalDamsel.getSource());
            withdrawal.setDestinationId(withdrawalDamsel.getDestination());
            withdrawal.setWithdrawalStatus(WithdrawalStatus.pending);

            withdrawal.setPartyId(wallet.getPartyId());
            withdrawal.setPartyContractId(wallet.getPartyContractId());
            withdrawal.setIdentityId(wallet.getIdentityId());

            Cash cash = withdrawalDamsel.getBody();
            withdrawal.setAmount(cash.getAmount());
            withdrawal.setCurrencyCode(cash.getCurrency().getSymbolicCode());

            withdrawalDao.updateNotCurrent(event.getSource());
            withdrawalDao.save(withdrawal);
            log.info("Withdrawal have been saved, eventId={}, walletId={}", event.getId(), event.getSource());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Wallet getWallet(SinkEvent event, String walletId) {
        Wallet wallet = walletDao.get(walletId);
        if (wallet == null) {
            throw new SinkEventNotFoundException(String.format("Wallet not found, destinationId='%s', walletId='%s'", event.getSource(), walletId));
        }
        return wallet;
    }
}
