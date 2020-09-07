package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.geck.serializer.kit.mock.MockMode;
import com.rbkmoney.geck.serializer.kit.mock.MockTBaseProcessor;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static io.github.benas.randombeans.api.EnhancedRandom.randomListOf;

public abstract class AbstractWithdrawalTestUtils extends AbstractTestUtils {

    public static MockTBaseProcessor mockTBaseProcessor = new MockTBaseProcessor(MockMode.ALL, 10, 1);

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

    protected void saveWithdrawalsDependencies() {
        for (Identity identity : createIdentities(identityId, partyId, contractId)) {
            identityDao.save(identity);
        }
        for (Wallet wallet : createWallets(identityId, partyId, contractId, walletId)) {
            walletDao.save(wallet);
        }
        for (Withdrawal withdrawal : createWithdrawals(identityId, partyId, contractId, walletId, getInFromToPeriodTime())) {
            withdrawalDao.save(withdrawal);
        }
    }

    protected Report createReport() {
        return createReport(partyId, contractId, getToTime(), getFromTime());
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

    private List<Wallet> createWallets(String identityId, String partyId, String contractId, String walletId) {
        List<Wallet> wallets = new ArrayList<>();
        for (Wallet wallet : randomListOf(2, Wallet.class)) {
            wallet.setWalletId(walletId);
            wallet.setPartyId(partyId);
            wallet.setPartyContractId(contractId);
            wallet.setIdentityId(identityId);
            wallets.add(wallet);
        }
        wallets.addAll(randomListOf(4, Wallet.class));
        return wallets;
    }

    private List<Withdrawal> createWithdrawals(String identityId, String partyId, String contractId, String walletId, LocalDateTime eventCreatedAtTime) {
        List<Withdrawal> withdrawals = new ArrayList<>();
        for (Withdrawal withdrawal : randomListOf(getExpectedSize(), Withdrawal.class)) {
            fillAsReportWithdrawal(identityId, partyId, contractId, walletId, eventCreatedAtTime, withdrawal);
            withdrawals.add(withdrawal);
        }
        Withdrawal filtered = random(Withdrawal.class);
        fillAsReportWithdrawal(identityId, partyId, contractId, walletId, eventCreatedAtTime, filtered);
        filtered.setCurrent(false);
        withdrawals.add(filtered);
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawals.add(withdrawal);
            withdrawal.setCurrent(true);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.pending);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawal.setPartyId(partyId);
            withdrawal.setPartyContractId(contractId);
            withdrawal.setIdentityId(identityId);
            withdrawal.setCurrencyCode("RUB");
            withdrawals.add(withdrawal);
            withdrawal.setCurrent(true);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_CREATED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawal.setCurrencyCode("RUB");
            withdrawals.add(withdrawal);
            withdrawal.setCurrent(true);
        }
        //filtered by time
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            fillAsReportWithdrawal(identityId, partyId, contractId, walletId, eventCreatedAtTime.minusDays(21), withdrawal);
            withdrawals.add(withdrawal);
        }
        return withdrawals;
    }

    private void fillAsReportWithdrawal(String identityId, String partyId, String contractId, String walletId, LocalDateTime eventCreatedAtTime, Withdrawal withdrawal) {
        withdrawal.setId(null);
        withdrawal.setWalletId(walletId);
        withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
        withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
        withdrawal.setEventCreatedAt(eventCreatedAtTime);
        withdrawal.setPartyId(partyId);
        withdrawal.setPartyContractId(contractId);
        withdrawal.setIdentityId(identityId);
        withdrawal.setCurrencyCode("RUB");
        withdrawal.setCurrent(true);
    }
}
