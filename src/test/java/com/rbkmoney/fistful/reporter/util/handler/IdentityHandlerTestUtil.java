package com.rbkmoney.fistful.reporter.util.handler;

import com.rbkmoney.fistful.identity.*;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.msgpack.Value;

import static io.github.benas.randombeans.api.EnhancedRandom.random;

public class IdentityHandlerTestUtil {

    public static MachineEvent createMachineEvent(String id) {
        return new MachineEvent()
                .setEventId(2L)
                .setSourceId(id)
                .setSourceNs("2")
                .setCreatedAt("2021-05-31T06:12:27Z")
                .setData(Value.bin(new ThriftSerializer<>().serialize("", createLevelChanged())));
    }

    public static TimestampedChange createLevelChanged() {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.level_changed("upd"));
    }

    public static TimestampedChange createEffectiveChallengeChanged() {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.effective_challenge_changed("upd"));
    }

    public static TimestampedChange createChallengeCreated(String id) {
        return new TimestampedChange()
                .setOccuredAt("2021-05-31T06:12:27Z")
                .setChange(Change.identity_challenge(new ChallengeChange(
                        id,
                        ChallengeChangePayload.created(random(Challenge.class, "proofs", "status"))
                )));
    }
}
