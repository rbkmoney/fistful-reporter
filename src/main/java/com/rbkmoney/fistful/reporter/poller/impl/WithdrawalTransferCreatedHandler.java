package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.cashflow.FinalCashFlowPosting;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalTransferStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.dto.FistfulCashFlowSinkEvent;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.WithdrawalEventHandler;
import com.rbkmoney.fistful.reporter.util.CashFlowConverter;
import com.rbkmoney.fistful.withdrawal.Change;
import com.rbkmoney.fistful.withdrawal.SinkEvent;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class WithdrawalTransferCreatedHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetTransfer() && change.getTransfer().isSetPayload() && change.getTransfer().getPayload().isSetCreated()
                && change.getTransfer().getPayload().getCreated().isSetTransfer() && change.getTransfer().getPayload().getCreated().getTransfer().isSetCashflow();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            List<FinalCashFlowPosting> postings = change.getTransfer().getPayload().getCreated().getTransfer().getCashflow().getPostings();

            log.info("Start withdrawal transfer created handling, eventId={}, walletId={}, transferChange={}", event.getId(), event.getSource(), change.getTransfer());
            Withdrawal withdrawal = withdrawalDao.get(event.getSource());

            withdrawal.setId(null);
            withdrawal.setWtime(null);

            withdrawal.setEventId(event.getId());
            withdrawal.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            withdrawal.setWithdrawalId(event.getSource());
            withdrawal.setSequenceId(event.getPayload().getSequence());
            withdrawal.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_TRANSFER_CREATED);
            withdrawal.setWithdrawalTransferStatus(WithdrawalTransferStatus.created);

            withdrawal.setFee(CashFlowConverter.getFistfulFee(postings));
            withdrawal.setProviderFee(CashFlowConverter.getFistfulProviderFee(postings));

            withdrawalDao.updateNotCurrent(event.getSource());
            long id = withdrawalDao.save(withdrawal);

            List<FistfulCashFlow> fistfulCashFlows = CashFlowConverter.convertFistfulCashFlows(
                    new FistfulCashFlowSinkEvent(
                            event.getId(),
                            event.getCreatedAt(),
                            event.getSource(),
                            event.getPayload().getSequence(),
                            event.getPayload().getOccuredAt(),
                            WithdrawalEventType.WITHDRAWAL_TRANSFER_CREATED.toString(),
                            id,
                            FistfulCashFlowChangeType.withdrawal,
                            postings
                    )
            );
            fistfulCashFlowDao.save(fistfulCashFlows);
            log.info("Withdrawal transfer has been saved, eventId={}, walletId={}, transferChange={}", event.getId(), event.getSource(), change.getTransfer());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
