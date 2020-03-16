package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.base.BankCard;
import com.rbkmoney.fistful.destination.*;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class DestinationSinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent create(String destinationId, String identityId) {
        List<Change> changes = asList(
                createCreatedChange(),
                createStatusChangedChange(),
                createAccountCreatedChange(identityId)
        );

        Event event = new Event(generateInt(), generateDate(), changes);

        return new SinkEvent(
                generateLong(),
                generateDate(),
                destinationId,
                event
        );
    }

    private static Change createAccountCreatedChange(String identityId) {
        Account account = random(Account.class);
        account.setIdentity(identityId);
        return Change.account(AccountChange.created(account));
    }

    private static Change createStatusChangedChange() {
        return Change.status(StatusChange.changed(Status.authorized(new Authorized())));
    }

    private static Change createCreatedChange() {
        BankCard bankCard = random(BankCard.class, BankCard._Fields.BIN_DATA_ID.getFieldName());
        return Change.created(
                new com.rbkmoney.fistful.destination.Destination(
                        generateString(),
                        Resource.bank_card(bankCard)
                )
        );
    }
}
