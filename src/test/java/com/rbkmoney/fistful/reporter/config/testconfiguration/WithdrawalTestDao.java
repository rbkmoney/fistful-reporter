package com.rbkmoney.fistful.reporter.config.testconfiguration;

import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import lombok.RequiredArgsConstructor;

import static com.rbkmoney.fistful.reporter.data.TestData.*;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getInFromToPeriodTime;

@RequiredArgsConstructor
public class WithdrawalTestDao {

    private final IdentityDao identityDao;
    private final WalletDao walletDao;
    private final WithdrawalDao withdrawalDao;

    public void saveWithdrawalsDependencies(int expectedSize) {
        for (Identity identity : createIdentities(identityId, partyId, contractId)) {
            identityDao.save(identity);
        }
        for (Wallet wallet : createWallets(identityId, partyId, contractId, walletId)) {
            walletDao.save(wallet);
        }
        for (Withdrawal withdrawal : createWithdrawals(
                identityId,
                partyId,
                contractId,
                walletId,
                getInFromToPeriodTime(),
                expectedSize)) {
            withdrawalDao.save(withdrawal);
        }
    }
}
