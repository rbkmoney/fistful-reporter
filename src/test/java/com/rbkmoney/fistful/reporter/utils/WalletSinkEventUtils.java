package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.AbstractUtils;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.wallet.AccountChange;
import com.rbkmoney.fistful.wallet.Change;
import com.rbkmoney.fistful.wallet.Event;
import com.rbkmoney.fistful.wallet.SinkEvent;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class WalletSinkEventUtils extends AbstractUtils {

    public static SinkEvent test(String walletId, String identityId) {
        List<Change> changes = asList(
                createCreatedChange(),
                createAccountCreatedChange(identityId)
        );

        Event event = new Event(generateInt(), generateDate(), changes);

        SinkEvent sinkEvent = new SinkEvent(
                generateLong(),
                generateDate(),
                walletId,
                event
        );
        return sinkEvent;
    }

    private static Change createCreatedChange() {
        return Change.created(random(com.rbkmoney.fistful.wallet.Wallet.class));
    }

    private static Change createAccountCreatedChange(String identityId) {
        Account account = random(Account.class);
        account.setIdentity(identityId);
        return Change.account(AccountChange.created(account));
    }
}