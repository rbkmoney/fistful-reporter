package com.rbkmoney.fistful.reporter.data;

import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalEventType;
import com.rbkmoney.fistful.reporter.domain.enums.WithdrawalStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.kafka.common.serialization.ThriftSerializer;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import com.rbkmoney.machinegun.eventsink.SinkEvent;
import com.rbkmoney.machinegun.msgpack.Value;
import org.apache.thrift.TBase;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.randomListOf;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.*;

public class TestData {

    public static final String identityId = generateString();
    public static final String partyId = generateString();
    public static final String contractId = generateString();
    public static final String walletId = generateString();

    public static List<Identity> createIdentities(String identityId, String partyId, String contractId) {
        List<Identity> identities = new ArrayList<>();
        for (Identity identity : randomListOf(2, Identity.class)) {
            identity.setPartyId(partyId);
            identity.setPartyContractId(contractId);
            identity.setIdentityId(identityId);
            identities.add(identity);
        }
        for (Identity identity : randomListOf(4, Identity.class)) {
            identity.setIdentityId(identityId);
            identities.add(identity);
        }
        identities.addAll(randomListOf(4, Identity.class));
        return identities;
    }

    public static List<Wallet> createWallets(String identityId, String partyId, String contractId, String walletId) {
        List<Wallet> wallets = new ArrayList<>();
        for (Wallet wallet : randomListOf(2, Wallet.class)) {
            wallet.setWalletId(walletId);
            wallet.setPartyId(partyId);
            wallet.setPartyContractId(contractId);
            wallet.setIdentityId(identityId);
            wallets.add(wallet);
        }
        wallets.addAll(randomListOf(4, Wallet.class));
        return wallets;
    }

    public static List<Withdrawal> createWithdrawals(
            String identityId,
            String partyId,
            String contractId,
            String walletId,
            LocalDateTime eventCreatedAtTime,
            int expectedSize) {
        List<Withdrawal> withdrawals = new ArrayList<>();
        for (Withdrawal withdrawal : randomListOf(expectedSize, Withdrawal.class)) {
            fillAsReportWithdrawal(identityId, partyId, contractId, walletId, eventCreatedAtTime, withdrawal);
            withdrawals.add(withdrawal);
        }
        Withdrawal filtered = random(Withdrawal.class);
        fillAsReportWithdrawal(identityId, partyId, contractId, walletId, eventCreatedAtTime, filtered);
        filtered.setCurrent(false);
        withdrawals.add(filtered);
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawals.add(withdrawal);
            withdrawal.setCurrent(true);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.pending);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawal.setPartyId(partyId);
            withdrawal.setPartyContractId(contractId);
            withdrawal.setIdentityId(identityId);
            withdrawal.setCurrencyCode("RUB");
            withdrawals.add(withdrawal);
            withdrawal.setCurrent(true);
        }
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            withdrawal.setId(null);
            withdrawal.setWalletId(walletId);
            withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
            withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_CREATED);
            withdrawal.setEventCreatedAt(eventCreatedAtTime);
            withdrawal.setCurrencyCode("RUB");
            withdrawals.add(withdrawal);
            withdrawal.setCurrent(true);
        }
        //filtered by time
        for (Withdrawal withdrawal : randomListOf(4, Withdrawal.class)) {
            fillAsReportWithdrawal(
                    identityId,
                    partyId,
                    contractId,
                    walletId,
                    eventCreatedAtTime.minusDays(21),
                    withdrawal);
            withdrawals.add(withdrawal);
        }
        return withdrawals;
    }

    public static Report createReport() {
        return createReport(partyId, contractId, getToTime(), getFromTime());
    }

    private static com.rbkmoney.fistful.reporter.domain.tables.pojos.Report createReport(
            String partyId,
            String contractId,
            LocalDateTime toTime,
            LocalDateTime fromTime) {
        Report report = new Report();
        report.setPartyId(partyId);
        report.setContractId(contractId);
        report.setToTime(toTime);
        report.setFromTime(fromTime);
        return report;
    }

    public static SinkEvent sinkEvent(MachineEvent machineEvent) {
        SinkEvent sinkEvent = new SinkEvent();
        sinkEvent.setEvent(machineEvent);
        return sinkEvent;
    }

    public static <T extends TBase> MachineEvent machineEvent(
            ThriftSerializer<T> depositChangeSerializer,
            T change) {
        return new MachineEvent()
                .setEventId(1L)
                .setSourceId("source_id")
                .setSourceNs("source_ns")
                .setCreatedAt("2016-03-22T06:12:27Z")
                .setData(Value.bin(depositChangeSerializer.serialize("", change)));
    }

    private static void fillAsReportWithdrawal(
            String identityId,
            String partyId,
            String contractId,
            String walletId,
            LocalDateTime eventCreatedAtTime,
            Withdrawal withdrawal) {
        withdrawal.setId(null);
        withdrawal.setWalletId(walletId);
        withdrawal.setWithdrawalStatus(WithdrawalStatus.succeeded);
        withdrawal.setEventType(WithdrawalEventType.WITHDRAWAL_STATUS_CHANGED);
        withdrawal.setEventCreatedAt(eventCreatedAtTime);
        withdrawal.setPartyId(partyId);
        withdrawal.setPartyContractId(contractId);
        withdrawal.setIdentityId(identityId);
        withdrawal.setCurrencyCode("RUB");
        withdrawal.setCurrent(true);
    }
}
