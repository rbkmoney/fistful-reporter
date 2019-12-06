package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.base.Cash;
import com.rbkmoney.fistful.deposit.Change;
import com.rbkmoney.fistful.deposit.SinkEvent;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.domain.enums.DepositEventType;
import com.rbkmoney.fistful.reporter.domain.enums.DepositStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Deposit;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.exception.SinkEventNotFoundException;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.DepositEventHandler;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DepositCreatedHandler implements DepositEventHandler {

    private final DepositDao depositDao;
    private final WalletDao walletDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetCreated() && change.getCreated().isSetDeposit();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            var depositDamsel = change.getCreated().getDeposit();

            log.info("Start deposit created handling, eventId={}, depositId={}", event.getId(), event.getSource());

            Wallet wallet = getWallet(event, depositDamsel.getWallet());

            Deposit deposit = new Deposit();

            deposit.setEventId(event.getId());
            deposit.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            deposit.setDepositId(event.getSource());
            deposit.setSequenceId(event.getPayload().getSequence());
            deposit.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            deposit.setEventType(DepositEventType.DEPOSIT_CREATED);
            deposit.setWalletId(depositDamsel.getWallet());
            deposit.setSourceId(depositDamsel.getSource());
            deposit.setDepositStatus(DepositStatus.pending);

            deposit.setPartyId(wallet.getPartyId());
            deposit.setPartyContractId(wallet.getPartyContractId());
            deposit.setIdentityId(wallet.getIdentityId());

            Cash cash = depositDamsel.getBody();
            deposit.setAmount(cash.getAmount());
            deposit.setCurrencyCode(cash.getCurrency().getSymbolicCode());

            depositDao.updateNotCurrent(event.getSource());
            depositDao.save(deposit);
            log.info("Deposit have been saved, eventId={}, depositId={}", event.getId(), event.getSource());
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
