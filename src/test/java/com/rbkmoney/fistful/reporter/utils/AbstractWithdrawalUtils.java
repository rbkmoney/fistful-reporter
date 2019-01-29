package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.AbstractUtils;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.DaoException;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.randomListOf;

public abstract class AbstractWithdrawalUtils extends AbstractUtils {

    protected String identityId = generateString();
    protected String partyId = generateString();
    protected String contractId = generateString();
    protected String walletId = generateString();
    protected Report report = createReport();

    @Autowired
    protected IdentityDao identityDao;

    @Autowired
    protected WalletDao walletDao;

    @Autowired
    protected WithdrawalDao withdrawalDao;

    protected abstract int getExpectedSize();

    protected void saveWithdrawalsDependencies() throws DaoException {
        for (Identity identity : createIdentities(identityId, partyId, contractId)) {
            identityDao.save(identity);
        }
        for (Wallet wallet : createWallets(identityId, walletId)) {
            walletDao.save(wallet);
        }
        for (Withdrawal withdrawal : createWithdrawals(walletId, inFromToPeriodTime)) {
            withdrawalDao.save(withdrawal);
        }
    }

    protected Report createReport() {
        return createReport(partyId, contractId, toTime, fromTime);
    }

    private com.rbkmoney.fistful.reporter.domain.tables.pojos.Report createReport(String partyId, String contractId, LocalDateTime toTime, LocalDateTime fromTime) {
        Report report = new Report();
        report.setPartyId(partyId);
        report.setContractId(contractId);
        report.setToTime(toTime);
        report.setFromTime(fromTime);
        return report;
    }

    private List<Identity> createIdentities(String identityId, String partyId, String contractId) {
        List<Identity> identities = new ArrayList<>();
        for (Identity identity : randomListOf(2, Identity.class)) {
            identity.setPartyId(partyId);
            identity.setPartyContractId(contractId);
            identity.setIdentityId(identityId);
            identities.add(identity);
        }
        for (Identity identity : randomListOf(4, Identity.class)) {
            identity.setIdentityId(identityId);
            identities.add(identity);
        }
        identities.addAll(randomListOf(4, Identity.class));
        return identities;
    }

    private List<Wallet> createWallets(String identityId, String walletId) {
        List<Wallet> wallets = new ArrayList<>();
        for (Wallet wallet : randomListOf(2, Wallet.class)) {
            wallet.setIdentityId(identityId);
            wallet.setWalletId(walletId);
            wallets.add(wallet);
        }
        wallets.addAll(randomListOf(4, Wallet.class));
        return wallets;
    }

    private List<Withdrawal> createWithdrawals(String walletId, LocalDateTime eventCreatedAtTime) {
        List<Withdrawal> withdrawals = new ArrayList<>();
        for (Withdrawal withdrawal : randomListOf(getExpectedSize(), Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawals.add(withdrawal);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawals.add(withdrawal);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.pending);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawals.add(withdrawal);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_CREATED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawals.add(withdrawal);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime.minusDays(21));
            withdrawals.add(withdrawal);
        }
        return withdrawals;
    }
}