package com.rbkmoney.fistful.reporter.util.handler;

import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.source.*;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.msgpack.Value;

public class SourceHandlerTestUtil {

    public static MachineEvent createMachineEvent(String id, Account account) {
        return new MachineEvent()
                .setEventId(2L)
                .setSourceId(id)
                .setSourceNs("2")
                .setCreatedAt("2021-05-31T06:12:27Z")
                .setData(Value.bin(new ThriftSerializer<>().serialize("", createStatusChanged(account))));
    }

    public static TimestampedChange createStatusChanged(Account account) {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.status(new StatusChange(Status.authorized(new Authorized()))));
    }
}
