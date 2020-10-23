package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class WithdrawalService {

    public static final int WITHDRAWAL_LIMIT = 5000;

    private final WithdrawalDao withdrawalDao;

    @Transactional(propagation = Propagation.REQUIRED)
    public List<Withdrawal> getSucceededLimitWithdrawals(Report report, long fromId) {
        try {
            log.info("Trying to get succeeded withdrawals, " +
                            "reportId={}, partyId={}, contractId={}, fromId={}",
                    report.getId(), report.getPartyId(), report.getContractId(), fromId);

            List<Withdrawal> withdrawals = withdrawalDao.getSucceededWithdrawals(report, fromId, WITHDRAWAL_LIMIT);

            log.info("{} succeeded withdrawals has been found, " +
                            "reportId={}, partyId={}, contractId={}, fromId={}",
                    withdrawals.size(), report.getId(), report.getPartyId(), report.getContractId(), fromId);

            return withdrawals;
        } catch (DaoException ex) {
            throw new StorageException("Failed to get succeeded withdrawals", ex);
        }
    }
}
