package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.easyway.AbstractTestUtils;
import com.rbkmoney.fistful.base.Cash;
import com.rbkmoney.fistful.cashflow.*;
import com.rbkmoney.fistful.transfer.*;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static java.util.Collections.singletonList;

public class TrasnferTestUtil extends AbstractTestUtils {

    public static com.rbkmoney.fistful.transfer.Change getCommitedPayload() {
        return new com.rbkmoney.fistful.transfer.Change(
                com.rbkmoney.fistful.transfer.Change.status_changed(
                        new com.rbkmoney.fistful.transfer.StatusChange(
                                com.rbkmoney.fistful.transfer.Status.committed(new Committed())
                        )
                )
        );
    }

    public static com.rbkmoney.fistful.transfer.Change getCancelledPayload() {
        return new com.rbkmoney.fistful.transfer.Change(
                com.rbkmoney.fistful.transfer.Change.status_changed(
                        new com.rbkmoney.fistful.transfer.StatusChange(
                                com.rbkmoney.fistful.transfer.Status.cancelled(new Cancelled())
                        )
                )
        );
    }

    public static Change getCashFlowPayload() {
        return new Change(
                Change.created(
                        new CreatedChange(
                                new Transfer(
                                        getFinalCashFlow()
                                )
                        )
                )
        );
    }

    private static FinalCashFlow getFinalCashFlow() {
        return new FinalCashFlow(
                singletonList(
                        new FinalCashFlowPosting(
                                new FinalCashFlowAccount(
                                        CashFlowAccount.merchant(MerchantCashFlowAccount.payout),
                                        generateString()
                                ),
                                new FinalCashFlowAccount(
                                        CashFlowAccount.provider(ProviderCashFlowAccount.settlement),
                                        generateString()
                                ),
                                random(Cash.class)
                        )
                )
        );
    }
}
