package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.base.*;
import com.rbkmoney.fistful.destination.*;

import java.util.List;
import java.util.UUID;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class DestinationSinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent create(String destinationId, String identityId) {
        List<Change> changes = asList(
                createCreatedChangeBankCard(),
                createCreatedChangeCryptoWallet(),
                createStatusChangedChange(),
                createAccountCreatedChange(identityId)
        );

        EventSinkPayload event = new EventSinkPayload(generateInt(), generateDate(), changes);

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

    private static Change createCreatedChangeBankCard() {
        BankCard bankCard = random(BankCard.class, BankCard._Fields.BIN_DATA_ID.getFieldName());
        return Change.created(
                new com.rbkmoney.fistful.destination.Destination(
                        generateString(),
                        Resource.bank_card(new ResourceBankCard(bankCard))
                )
        );
    }

    private static Change createCreatedChangeCryptoWallet() {
        CryptoWallet cryptoWallet = new CryptoWallet();
        cryptoWallet.setId(UUID.randomUUID().toString());
        cryptoWallet.setCurrency(CryptoCurrency.bitcoin);
        cryptoWallet.setData(CryptoData.bitcoin(new CryptoDataBitcoin()));
        return Change.created(
                new com.rbkmoney.fistful.destination.Destination(
                        generateString(),
                        Resource.crypto_wallet(new ResourceCryptoWallet(cryptoWallet))
                )
        );
    }
}
