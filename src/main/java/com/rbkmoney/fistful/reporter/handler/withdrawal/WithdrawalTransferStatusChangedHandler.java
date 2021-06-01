package com.rbkmoney.fistful.reporter.handler.withdrawal;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.*;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.transfer.Status;
import com.rbkmoney.fistful.withdrawal.TimestampedChange;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WithdrawalTransferStatusChangedHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetTransfer()
                && change.getChange().getTransfer().isSetPayload()
                && change.getChange().getTransfer().getPayload().isSetStatusChanged()
                && change.getChange().getTransfer().getPayload().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {

            log.info("Start withdrawal transfer status changed handling, " +
                            "eventId={}, withdrawalId={}, transferChange={}",
                    event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
            Withdrawal oldWithdrawal = withdrawalDao.get(event.getSourceId());
            Withdrawal updatedWithdrawal = update(oldWithdrawal, change, event);
            withdrawalDao.save(updatedWithdrawal).ifPresentOrElse(
                    id -> {
                        withdrawalDao.updateNotCurrent(oldWithdrawal.getId());
                        List<FistfulCashFlow> cashFlows = fistfulCashFlowDao.getByObjId(
                                id,
                                FistfulCashFlowChangeType.withdrawal);
                        fillCashFlows(
                                cashFlows,
                                event,
                                WithdrawalEventType.WITHDRAWAL_TRANSFER_STATUS_CHANGED,
                                id,
                                change);
                        fistfulCashFlowDao.save(cashFlows);
                        log.info("Withdrawal transfer status have been status changed, " +
                                        "eventId={}, withdrawalId={}, transferChange={}",
                                event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
                    },
                    () -> log.info("Withdrawal transfer status change bound duplicated, " +
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
        withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_TRANSFER_STATUS_CHANGED);
        Status status = change.getChange().getTransfer().getPayload().getStatusChanged().getStatus();
        withdrawal.setWithdrawalTransferStatus(TBaseUtil.unionFieldToEnum(status, WithdrawalTransferStatus.class));
        return withdrawal;
    }
}
