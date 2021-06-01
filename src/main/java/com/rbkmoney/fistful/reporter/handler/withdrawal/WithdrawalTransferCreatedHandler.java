package com.rbkmoney.fistful.reporter.handler.withdrawal;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.cashflow.FinalCashFlowPosting;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.*;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.dto.FistfulCashFlowSinkEvent;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.util.CashFlowConverter;
import com.rbkmoney.fistful.withdrawal.TimestampedChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalTransferCreatedHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetTransfer()
                && change.getChange().getTransfer().isSetPayload()
                && change.getChange().getTransfer().getPayload().isSetCreated()
                && change.getChange().getTransfer().getPayload().getCreated().isSetTransfer()
                && change.getChange().getTransfer().getPayload().getCreated().getTransfer().isSetCashflow();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start withdrawal transfer created handling, eventId={}, withdrawalId={}, transferChange={}",
                    event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
            Withdrawal oldWithdrawal = withdrawalDao.get(event.getSourceId());
            Withdrawal updatedWithdrawal = update(oldWithdrawal, change, event);
            List<FinalCashFlowPosting> postings = change.getChange().getTransfer().getPayload()
                    .getCreated().getTransfer().getCashflow().getPostings();
            withdrawalDao.save(updatedWithdrawal).ifPresentOrElse(
                    id -> {
                        withdrawalDao.updateNotCurrent(oldWithdrawal.getId());
                        List<FistfulCashFlow> fistfulCashFlows = CashFlowConverter.convertFistfulCashFlows(
                                new FistfulCashFlowSinkEvent(
                                        event.getEventId(),
                                        event.getCreatedAt(),
                                        event.getSourceId(),
                                        change.getOccuredAt(),
                                        WithdrawalEventType.WITHDRAWAL_TRANSFER_CREATED.toString(),
                                        id,
                                        FistfulCashFlowChangeType.withdrawal,
                                        postings
                                )
                        );
                        fistfulCashFlowDao.save(fistfulCashFlows);
                        log.info("Withdrawal transfer have been created, " +
                                        "eventId={}, withdrawalId={}, transferChange={}",
                                event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
                    },
                    () -> log.info("Withdrawal transfer create bound duplicated, " +
                                    "eventId={}, withdrawalId={}, transferChange={}",
                            event.getEventId(), event.getSourceId(), change.getChange().getTransfer()));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Withdrawal update(
            Withdrawal oldWithdrawal,
            TimestampedChange change,
            MachineEvent event) {
        Withdrawal withdrawal = new Withdrawal(oldWithdrawal);
        withdrawal.setId(null);
        withdrawal.setWtime(null);
        withdrawal.setEventId(event.getEventId());
        withdrawal.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        withdrawal.setWithdrawalId(event.getSourceId());
        withdrawal.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_TRANSFER_CREATED);
        withdrawal.setWithdrawalTransferStatus(WithdrawalTransferStatus.created);
        List<FinalCashFlowPosting> postings = change.getChange().getTransfer().getPayload()
                .getCreated().getTransfer().getCashflow().getPostings();
        withdrawal.setFee(CashFlowConverter.getFistfulFee(postings));
        withdrawal.setProviderFee(CashFlowConverter.getFistfulProviderFee(postings));
        return withdrawal;
    }
}
