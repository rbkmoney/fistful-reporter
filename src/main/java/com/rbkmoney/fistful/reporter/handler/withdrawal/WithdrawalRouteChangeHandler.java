package com.rbkmoney.fistful.reporter.handler.withdrawal;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.exception.StorageException;
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
public class WithdrawalRouteChangeHandler implements WithdrawalEventHandler {

    private final WithdrawalDao withdrawalDao;
    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetRoute() && change.getChange().getRoute().isSetRoute();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start withdrawal provider id changed handling, eventId={}, withdrawalId={}",
                    event.getEventId(), event.getSourceId());
            Withdrawal withdrawal = withdrawalDao.get(event.getSourceId());
            withdrawal.setId(null);
            withdrawal.setWtime(null);
            withdrawal.setEventId(event.getEventId());
            withdrawal.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            withdrawal.setWithdrawalId(event.getSourceId());
            withdrawal.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_ROUTE_CHANGED);

            Long oldId = withdrawal.getId();
            withdrawalDao.save(withdrawal).ifPresentOrElse(
                    id -> {
                        withdrawalDao.updateNotCurrent(oldId);
                        List<FistfulCashFlow> cashFlows = fistfulCashFlowDao.getByObjId(withdrawal.getId(),
                                FistfulCashFlowChangeType.withdrawal);
                        fillCashFlows(cashFlows, event, WithdrawalEventType.WITHDRAWAL_ROUTE_CHANGED, id, change);
                        fistfulCashFlowDao.save(cashFlows);
                        log.info("Withdrawal provider id has been changed, eventId={}, withdrawalId={}",
                                event.getEventId(), event.getSourceId());
                    },
                    () -> log.info("Withdrawal provider id change bound duplicated, eventId={}, withdrawalId={}",
                            event.getEventId(), event.getSourceId()));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
