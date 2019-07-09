package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.identity.*;
import com.rbkmoney.geck.serializer.kit.tbase.TBaseHandler;
import lombok.SneakyThrows;

import java.util.List;

import static com.rbkmoney.fistful.reporter.utils.AbstractWithdrawalTestUtils.mockTBaseProcessor;
import static java.util.Arrays.asList;

public class IdentitySinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent create(String identityId) {
        String challengeId = generateString();

        List<Change> changes = asList(
                createCreatedChange(),
                createLevelChangedChange(),
                createChallengeCreatedChange(challengeId),
                createChallengeStatusChangedChange(challengeId),
                createEffectiveChallengeChangedChange()
        );

        Event event = new Event(generateInt(), generateDate(), changes);

        return new SinkEvent(
                generateLong(),
                generateDate(),
                identityId,
                event
        );
    }

    @SneakyThrows
    private static Change createCreatedChange() {
        return Change.created(mockTBaseProcessor.process(new Identity(), new TBaseHandler<>(Identity.class)));
    }

    private static Change createLevelChangedChange() {
        return Change.level_changed(generateString());
    }

    @SneakyThrows
    private static Change createChallengeCreatedChange(String challengeId) {
        return Change.identity_challenge(
                new ChallengeChange(
                        challengeId,
                        ChallengeChangePayload.created(mockTBaseProcessor.process(new Challenge(), new TBaseHandler<>(Challenge.class)))
                )
        );
    }

    private static Change createChallengeStatusChangedChange(String challengeId) {
        return Change.identity_challenge(
                new ChallengeChange(
                        challengeId,
                        ChallengeChangePayload.status_changed(
                                ChallengeStatus.cancelled(new ChallengeCancelled())
                        )
                )
        );
    }

    private static Change createEffectiveChallengeChangedChange() {
        return Change.effective_challenge_changed(generateString());
    }
}
