package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.deposit.*;
import com.rbkmoney.fistful.deposit.status.Status;
import com.rbkmoney.fistful.deposit.status.Succeeded;

import java.util.List;

import static com.rbkmoney.fistful.reporter.utils.TrasnferTestUtil.getCashFlowPayload;
import static com.rbkmoney.fistful.reporter.utils.TrasnferTestUtil.getCommitedPayload;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class DepositSinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent create(String depositId, String walletId) {
        List<Change> changes = asList(
                createCreatedChange(walletId),
                createStatusChangedChange(),
                createTransferCreatedChange(),
                createTransferStatusChangedChange()
        );
        Event event = new Event(generateInt(), generateDate(), changes);

        return new SinkEvent(
                generateLong(),
                generateDate(),
                depositId,
                event
        );
    }

    private static Change createCreatedChange(String walletId) {
        Deposit deposit = random(Deposit.class, "status");
        deposit.setWallet(walletId);
        return Change.created(new CreatedChange(deposit));
    }

    private static Change createStatusChangedChange() {
        return Change.status_changed(new StatusChange(Status.succeeded(new Succeeded())));
    }

    private static Change createTransferStatusChangedChange() {
        return Change.transfer(new TransferChange(getCommitedPayload()));
    }

    private static Change createTransferCreatedChange() {
        return Change.transfer(new TransferChange(getCashFlowPayload()));
    }
}
