package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;

import java.util.List;

public interface WithdrawalService {

    List<Withdrawal> getSucceededWithdrawalsByReport(Report report);
}
