package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;

import java.util.List;

public interface WithdrawalDao extends FistfulDao<Withdrawal> {

    List<Withdrawal> getSucceededWithdrawalsByReport(Report report, Long fromId, int limit);

}
