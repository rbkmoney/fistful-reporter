package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.source.*;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class SourceSinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent create(String sourceId, String identityId) {
        List<Change> changes = asList(
                createCreatedChange(),
                createStatusChangedChange(),
                createAccountCreatedChange(identityId)
        );

        Event event = new Event(generateInt(), generateDate(), changes);

        return new SinkEvent(
                generateLong(),
                generateDate(),
                sourceId,
                event
        );
    }

    private static Change createCreatedChange() {
        return Change.created(
                new com.rbkmoney.fistful.source.Source(
                        generateString(),
                        Resource.internal(new Internal())
                )
        );
    }

    private static Change createStatusChangedChange() {
        return Change.status(StatusChange.changed(Status.authorized(new Authorized())));
    }

    private static Change createAccountCreatedChange(String identityId) {
        Account account = random(Account.class);
        account.setIdentity(identityId);
        return Change.account(AccountChange.created(account));
    }
}
