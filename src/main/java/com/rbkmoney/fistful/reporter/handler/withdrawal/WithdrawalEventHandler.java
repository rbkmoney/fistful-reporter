package com.rbkmoney.fistful.reporter.handler.withdrawal;

import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.handler.EventHandler;
import com.rbkmoney.fistful.withdrawal.TimestampedChange;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;

import java.util.List;

public interface WithdrawalEventHandler extends EventHandler<TimestampedChange, MachineEvent> {

    default void fillCashFlows(
            List<FistfulCashFlow> cashFlows,
            MachineEvent event,
            WithdrawalEventType withdrawalEventType,
            long id,
            TimestampedChange change) {
        cashFlows.forEach(
                pcf -> {
                    pcf.setId(null);
                    pcf.setEventId(event.getEventId());
                    pcf.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
                    pcf.setSourceId(event.getSourceId());
                    pcf.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
                    pcf.setEventType(withdrawalEventType.toString());
                    pcf.setObjId(id);
                    pcf.setObjType(FistfulCashFlowChangeType.withdrawal);
                }
        );
    }
}
