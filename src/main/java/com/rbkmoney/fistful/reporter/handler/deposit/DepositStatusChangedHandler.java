package com.rbkmoney.fistful.reporter.handler.deposit;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.deposit.TimestampedChange;
import com.rbkmoney.fistful.deposit.status.Status;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.domain.enums.*;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Deposit;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.exception.StorageException;
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
public class DepositStatusChangedHandler implements DepositEventHandler {

    private final DepositDao depositDao;
    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetStatusChanged() && change.getChange().getStatusChanged().isSetStatus();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            Status status = change.getChange().getStatusChanged().getStatus();
            log.info("Start deposit status changed handling, eventId={}, depositId={}, status={}",
                    event.getEventId(), event.getSourceId(), status);
            Deposit oldDeposit = depositDao.get(event.getSourceId());
            Deposit updatedDeposit = update(oldDeposit, change, event, status);
            depositDao.save(updatedDeposit).ifPresentOrElse(
                    id -> {
                        depositDao.updateNotCurrent(oldDeposit.getId());
                        List<FistfulCashFlow> cashFlows = fistfulCashFlowDao.getByObjId(
                                id,
                                FistfulCashFlowChangeType.deposit);
                        fillCashFlows(cashFlows, event, DepositEventType.DEPOSIT_STATUS_CHANGED, change, id);
                        fistfulCashFlowDao.save(cashFlows);
                        log.info("Deposit status has been changed, eventId={}, depositId={}, status={}",
                                event.getEventId(), event.getSourceId(), status);
                    },
                    () -> log.info("Deposit status change bound duplicated,  eventId={}, depositId={}, status={}",
                            event.getEventId(), event.getSourceId(), status)
            );
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Deposit update(
            Deposit oldDeposit,
            TimestampedChange change,
            MachineEvent event,
            Status status) {
        Deposit deposit = new Deposit(oldDeposit);
        deposit.setId(null);
        deposit.setWtime(null);
        deposit.setEventId(event.getEventId());
        deposit.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        deposit.setDepositId(event.getSourceId());
        deposit.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        deposit.setEventType(DepositEventType.DEPOSIT_STATUS_CHANGED);
        deposit.setDepositStatus(TBaseUtil.unionFieldToEnum(status, DepositStatus.class));
        return deposit;
    }
}
