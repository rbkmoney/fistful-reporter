package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.withdrawal.*;
import com.rbkmoney.fistful.withdrawal.status.Failed;
import com.rbkmoney.fistful.withdrawal.status.Status;

import java.util.List;

import static com.rbkmoney.fistful.reporter.utils.TrasnferTestUtil.getCancelledPayload;
import static com.rbkmoney.fistful.reporter.utils.TrasnferTestUtil.getCashFlowPayload;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Arrays.asList;

public class WithdrawalSinkEventTestUtils extends AbstractTestUtils {

    public static SinkEvent create(String withdrawalId, String walletId) {
        List<Change> changes = asList(
                createCreatedChange(walletId),
                createStatusChangedChange(),
                createTransferCreatedChange(),
                createTransferStatusChangedChange(),
                createRouteChangedChange()
        );

        EventSinkPayload event = new EventSinkPayload(generateInt(), generateDate(), changes);

        return new SinkEvent(
                generateLong(),
                generateDate(),
                withdrawalId,
                event
        );
    }

    private static Change createCreatedChange(String walletId) {
        Withdrawal withdrawal = random(Withdrawal.class, "context", "status");
        withdrawal.setId(walletId);
        return Change.created(new CreatedChange(withdrawal));
    }

    private static Change createStatusChangedChange() {
        return Change.status_changed(new StatusChange(Status.failed(new Failed())));
    }

    private static Change createTransferCreatedChange() {
        return Change.transfer(new TransferChange(getCashFlowPayload()));

    }

    private static Change createTransferStatusChangedChange() {
        return Change.transfer(new TransferChange(getCancelledPayload()));

    }

    private static Change createRouteChangedChange() {
        return Change.route(new RouteChange(new Route(generateString())));
    }
}
