package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.base.CryptoCurrency;
import com.rbkmoney.fistful.base.CryptoData;
import com.rbkmoney.fistful.base.CryptoDataBitcoin;
import com.rbkmoney.fistful.base.CryptoWallet;
import com.rbkmoney.fistful.destination.*;

import java.util.List;
import java.util.UUID;

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
        CryptoWallet cryptoWallet = new CryptoWallet(UUID.randomUUID().toString(), CryptoCurrency.bitcoin);
        cryptoWallet.setData(CryptoData.bitcoin(new CryptoDataBitcoin()));
        return Change.created(
                new com.rbkmoney.fistful.destination.Destination(
                        generateString(),
                        Resource.crypto_wallet(cryptoWallet)
                )
        );
    }
}
