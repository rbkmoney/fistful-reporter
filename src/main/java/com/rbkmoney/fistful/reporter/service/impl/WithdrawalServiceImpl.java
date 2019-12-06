package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.service.WithdrawalService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class WithdrawalServiceImpl implements WithdrawalService {

    private final WithdrawalDao withdrawalDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public List<Withdrawal> getSucceededWithdrawalsByReport(Report report) {
        try {
            log.info("Trying to get succeeded withdrawals by report, " +
                            "reportId={}, partyId={}, contractId={}",
                    report.getId(), report.getPartyId(), report.getContractId()
            );
            long fromId = 0L;
            int limit = 1000;
            List<Withdrawal> pendingReports = withdrawalDao.getSucceededWithdrawalsByReport(report, fromId, limit);
            List<Withdrawal> allPendingReports = new ArrayList<>(pendingReports);

            while (pendingReports.size() == limit) {
                fromId = pendingReports.get(pendingReports.size() - 1).getId();
                pendingReports = withdrawalDao.getSucceededWithdrawalsByReport(report, fromId, limit);
                allPendingReports.addAll(pendingReports);
            }
            log.info("{} succeeded withdrawals by report have been found", allPendingReports.size());
            Comparator<Withdrawal> comparing = Comparator.comparing(Withdrawal::getEventCreatedAt);
            return allPendingReports.stream()
                    .sorted(comparing.reversed())
                    .collect(Collectors.toList());
        } catch (DaoException ex) {
            throw new StorageException("Failed to get pending reports", ex);
        }
    }
}
