package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.wallet.*;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class WalletSinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent test(String walletId, String identityId) {
        List<Change> changes = asList(
                createCreatedChange(),
                createAccountCreatedChange(identityId)
        );

        Event event = new Event(generateInt(), generateDate(), changes);

        return new SinkEvent(
                generateLong(),
                generateDate(),
                walletId,
                event
        );
    }

    private static Change createCreatedChange() {
        return Change.created(random(Wallet.class, "context", "account"));
    }

    private static Change createAccountCreatedChange(String identityId) {
        Account account = random(Account.class);
        account.setIdentity(identityId);
        return Change.account(AccountChange.created(account));
    }
}
