package com.rbkmoney.fistful.reporter.handler.withdrawal;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.*;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.withdrawal.TimestampedChange;
import com.rbkmoney.fistful.withdrawal.status.Status;
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
public class WithdrawalStatusChangedHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatusChanged() && change.getChange().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start withdrawal status changed handling, eventId={}, withdrawalId={}, status={}",
                    event.getEventId(), event.getSourceId(), change.getChange().getStatusChanged());
            Withdrawal oldWithdrawal = withdrawalDao.get(event.getSourceId());
            Withdrawal updatedWithdrawal = update(oldWithdrawal, change, event);
            withdrawalDao.save(updatedWithdrawal).ifPresentOrElse(
                    id -> {
                        withdrawalDao.updateNotCurrent(oldWithdrawal.getId());
                        List<FistfulCashFlow> cashFlows = fistfulCashFlowDao.getByObjId(
                                updatedWithdrawal.getId(),
                                FistfulCashFlowChangeType.withdrawal);
                        fillCashFlows(cashFlows, event, WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED, id, change);
                        fistfulCashFlowDao.save(cashFlows);
                        log.info("Withdrawal status have been changed, eventId={}, withdrawalId={}, status={}",
                                event.getEventId(), event.getSourceId(), change.getChange().getStatusChanged());
                    },
                    () -> log.info("Withdrawal status change bound duplicated, eventId={}, withdrawalId={}, status={}",
                            event.getEventId(), event.getSourceId(), change.getChange().getStatusChanged()));
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
        withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
        Status status = change.getChange().getStatusChanged().getStatus();
        withdrawal.setWithdrawalStatus(TBaseUtil.unionFieldToEnum(status, WithdrawalStatus.class));
        return withdrawal;
    }
}
