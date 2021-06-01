package com.rbkmoney.fistful.reporter.handler.deposit;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.deposit.TimestampedChange;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.domain.enums.*;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Deposit;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.transfer.Status;
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
public class DepositTransferStatusChangedHandler implements DepositEventHandler {

    private final DepositDao depositDao;
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
            log.info("Start deposit transfer status changed handling, eventId={}, depositId={}, transferChange={}",
                    event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
            Deposit oldDeposit = depositDao.get(event.getSourceId());
            Deposit updatedDeposit = update(oldDeposit, change, event);
            depositDao.save(updatedDeposit).ifPresentOrElse(
                    id -> {
                        depositDao.updateNotCurrent(oldDeposit.getId());
                        List<FistfulCashFlow> cashFlows = fistfulCashFlowDao.getByObjId(
                                updatedDeposit.getId(),
                                FistfulCashFlowChangeType.deposit);
                        fillCashFlows(cashFlows, event, DepositEventType.DEPOSIT_TRANSFER_STATUS_CHANGED, change, id);
                        fistfulCashFlowDao.save(cashFlows);
                        log.info("Deposit transfer status has been changed, " +
                                        "eventId={}, depositId={}, transferChange={}",
                                event.getEventId(), event.getSourceId(), change.getChange().getTransfer());
                    },
                    () -> log.info("Deposit transfer status change bound duplicated, " +
                                    "eventId={}, depositId={}, transferChange={}",
                            event.getEventId(), event.getSourceId(), change.getChange().getTransfer())
            );
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private Deposit update(
            Deposit oldDeposit,
            TimestampedChange change,
            MachineEvent event) {
        Deposit deposit = new Deposit(oldDeposit);
        deposit.setId(null);
        deposit.setWtime(null);
        deposit.setEventId(event.getEventId());
        deposit.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        deposit.setDepositId(event.getSourceId());
        deposit.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        deposit.setEventType(DepositEventType.DEPOSIT_TRANSFER_STATUS_CHANGED);
        Status status = change.getChange().getTransfer().getPayload().getStatusChanged().getStatus();
        deposit.setDepositTransferStatus(TBaseUtil.unionFieldToEnum(status, DepositTransferStatus.class));
        return deposit;
    }
}
