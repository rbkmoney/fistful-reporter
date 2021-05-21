package com.rbkmoney.fistful.reporter.handler.deposit;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.deposit.TimestampedChange;
import com.rbkmoney.fistful.deposit.status.Status;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.domain.enums.DepositEventType;
import com.rbkmoney.fistful.reporter.domain.enums.DepositStatus;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
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

            Deposit deposit = depositDao.get(event.getSourceId());
            deposit.setId(null);
            deposit.setWtime(null);
            deposit.setEventId(event.getEventId());
            deposit.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            deposit.setDepositId(event.getSourceId());
            deposit.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            deposit.setEventType(DepositEventType.DEPOSIT_STATUS_CHANGED);
            deposit.setDepositStatus(TBaseUtil.unionFieldToEnum(status, DepositStatus.class));

            Long oldId = deposit.getId();
            depositDao.save(deposit).ifPresentOrElse(
                    id -> {
                        depositDao.updateNotCurrent(oldId);
                        List<FistfulCashFlow> cashFlows = fistfulCashFlowDao.getByObjId(
                                deposit.getId(),
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
}
