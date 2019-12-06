package com.rbkmoney.fistful.reporter.util;

import com.rbkmoney.fistful.cashflow.FinalCashFlowPosting;
import com.rbkmoney.fistful.reporter.domain.enums.DepositEventType;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowAccount;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.dto.FistfulCashFlowSinkEvent;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CashFlowConverter {

    public static List<FistfulCashFlow> convertFistfulCashFlows(FistfulCashFlowSinkEvent event) {
        return event.getPostings().stream()
                .map(
                        cf -> {
                            FistfulCashFlow fcf = new FistfulCashFlow();

                            fcf.setEventId(event.getEventId());
                            fcf.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getEventCreatedAt()));
                            fcf.setSourceId(event.getSourceId());
                            fcf.setSequenceId(event.getSequenceId());
                            fcf.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getEventOccuredAt()));
                            fcf.setEventType(DepositEventType.DEPOSIT_STATUS_CHANGED.toString());
                            fcf.setObjId(event.getObjId());
                            fcf.setObjType(event.getCashFlowChangeType());
                            fcf.setSourceAccountType(TBaseUtil.unionFieldToEnum(cf.getSource().getAccountType(), FistfulCashFlowAccount.class));
                            fcf.setSourceAccountTypeValue(getCashFlowAccountTypeValue(cf.getSource()));
                            fcf.setSourceAccountId(cf.getSource().getAccountId());
                            fcf.setDestinationAccountType(TBaseUtil.unionFieldToEnum(cf.getDestination().getAccountType(), FistfulCashFlowAccount.class));
                            fcf.setDestinationAccountTypeValue(getCashFlowAccountTypeValue(cf.getDestination()));
                            fcf.setDestinationAccountId(cf.getDestination().getAccountId());
                            fcf.setAmount(cf.getVolume().getAmount());
                            fcf.setCurrencyCode(cf.getVolume().getCurrency().getSymbolicCode());
                            fcf.setDetails(cf.getDetails());
                            return fcf;
                        }
                )
                .collect(Collectors.toList());
    }

    public static long getFistfulFee(List<FinalCashFlowPosting> postings) {
        return getFistfulAmount(
                postings,
                posting -> posting.getSource().getAccountType().isSetWallet()
                        && posting.getDestination().getAccountType().isSetSystem()
        );
    }

    public static long getFistfulProviderFee(List<com.rbkmoney.fistful.cashflow.FinalCashFlowPosting> postings) {
        return getFistfulAmount(
                postings,
                posting -> posting.getSource().getAccountType().isSetSystem()
                        && posting.getDestination().getAccountType().isSetProvider()
        );
    }

    public static long getFistfulAmount(List<FinalCashFlowPosting> postings,
                                        Predicate<FinalCashFlowPosting> filter) {
        return postings.stream()
                .filter(filter)
                .map(posting -> posting.getVolume().getAmount())
                .reduce(0L, Long::sum);
    }

    private static String getCashFlowAccountTypeValue(com.rbkmoney.fistful.cashflow.FinalCashFlowAccount cfa) {
        if (cfa.getAccountType().isSetMerchant()) {
            return cfa.getAccountType().getMerchant().name();
        } else if (cfa.getAccountType().isSetProvider()) {
            return cfa.getAccountType().getProvider().name();
        } else if (cfa.getAccountType().isSetSystem()) {
            return cfa.getAccountType().getSystem().name();
        } else if (cfa.getAccountType().isSetExternal()) {
            return cfa.getAccountType().getExternal().name();
        } else if (cfa.getAccountType().isSetWallet()) {
            return cfa.getAccountType().getWallet().name();
        } else {
            throw new IllegalArgumentException("Illegal fistful cash flow account type: " + cfa.getAccountType());
        }
    }
}
