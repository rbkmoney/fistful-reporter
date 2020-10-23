package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.cashflow.FinalCashFlowPosting;
import com.rbkmoney.fistful.deposit.Change;
import com.rbkmoney.fistful.deposit.SinkEvent;
import com.rbkmoney.fistful.reporter.dao.DepositDao;
import com.rbkmoney.fistful.reporter.dao.FistfulCashFlowDao;
import com.rbkmoney.fistful.reporter.domain.enums.DepositEventType;
import com.rbkmoney.fistful.reporter.domain.enums.DepositTransferStatus;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Deposit;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.FistfulCashFlow;
import com.rbkmoney.fistful.reporter.dto.FistfulCashFlowSinkEvent;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.DepositEventHandler;
import com.rbkmoney.fistful.reporter.util.CashFlowConverter;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class DepositTransferCreatedHandler implements DepositEventHandler {

    private final DepositDao depositDao;

    private final FistfulCashFlowDao fistfulCashFlowDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetTransfer() && change.getTransfer().isSetPayload() && change.getTransfer().getPayload().isSetCreated()
                && change.getTransfer().getPayload().getCreated().isSetTransfer() && change.getTransfer().getPayload().getCreated().getTransfer().isSetCashflow();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            List<FinalCashFlowPosting> postings = change.getTransfer().getPayload().getCreated().getTransfer().getCashflow().getPostings();

            log.info("Start deposit transfer created handling, eventId={}, depositId={}, transferChange={}", event.getId(), event.getSource(), change.getTransfer());

            Deposit deposit = depositDao.get(event.getSource());

            deposit.setId(null);
            deposit.setWtime(null);

            deposit.setEventId(event.getId());
            deposit.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            deposit.setDepositId(event.getSource());
            deposit.setSequenceId(event.getPayload().getSequence());
            deposit.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            deposit.setEventType(DepositEventType.DEPOSIT_TRANSFER_CREATED);
            deposit.setDepositTransferStatus(DepositTransferStatus.created);

            deposit.setFee(CashFlowConverter.getFistfulFee(postings));
            deposit.setProviderFee(CashFlowConverter.getFistfulProviderFee(postings));

            depositDao.updateNotCurrent(event.getSource());
            long id = depositDao.save(deposit);

            List<FistfulCashFlow> fistfulCashFlows = CashFlowConverter.convertFistfulCashFlows(
                    new FistfulCashFlowSinkEvent(
                            event.getId(),
                            event.getCreatedAt(),
                            event.getSource(),
                            event.getPayload().getSequence(),
                            event.getPayload().getOccuredAt(),
                            DepositEventType.DEPOSIT_TRANSFER_CREATED.toString(),
                            id,
                            FistfulCashFlowChangeType.deposit,
                            postings
                    )
            );
            fistfulCashFlowDao.save(fistfulCashFlows);
            log.info("Deposit transfer has been saved, eventId={}, depositId={}, transferChange={}", event.getId(), event.getSource(), change.getTransfer());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
