package com.rbkmoney.fistful.reporter.utils.handler;

import com.rbkmoney.fistful.withdrawal.*;
import com.rbkmoney.fistful.withdrawal.status.Status;
import com.rbkmoney.fistful.withdrawal.status.Succeeded;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.msgpack.Value;

import static com.rbkmoney.fistful.reporter.utils.TransferTestUtil.getCashFlowPayload;
import static com.rbkmoney.fistful.reporter.utils.TransferTestUtil.getCommitedPayload;

public class WithdrawalHandlerTestUtils {

    public static MachineEvent createMachineEvent(String id) {
        return new MachineEvent()
                .setEventId(2L)
                .setSourceId(id)
                .setSourceNs("2")
                .setCreatedAt("2021-05-31T06:12:27Z")
                .setData(Value.bin(new ThriftSerializer<>().serialize("", createStatusChanged())));
    }

    public static TimestampedChange createStatusChanged() {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.status_changed(
                        new StatusChange().setStatus(
                                Status.succeeded(new Succeeded()))));
    }

    public static TimestampedChange createTransferChanged() {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.transfer(new TransferChange(getCommitedPayload())));
    }

    public static TimestampedChange createTransferCreated() {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.transfer(new TransferChange(getCashFlowPayload())));
    }


}
