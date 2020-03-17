package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.base.BankCard;
import com.rbkmoney.fistful.base.CryptoWallet;
import com.rbkmoney.fistful.base.Resource;
import com.rbkmoney.fistful.destination.Change;
import com.rbkmoney.fistful.destination.SinkEvent;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationEventType;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationResourceType;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.DestinationEventHandler;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class DestinationCreatedHandler implements DestinationEventHandler {

    private final DestinationDao destinationDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            log.info("Start destination created handling, eventId={}, destinationId={}", event.getId(), event.getSource());
            Destination destination = new Destination();

            destination.setEventId(event.getId());
            destination.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            destination.setDestinationId(event.getSource());
            destination.setSequenceId(event.getPayload().getSequence());
            destination.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
            destination.setEventType(DestinationEventType.DESTINATION_CREATED);
            destination.setDestinationName(change.getCreated().getName());
            destination.setDestinationStatus(DestinationStatus.unauthorized);

            Resource resource = change.getCreated().getResource();
            if (resource.isSetBankCard()) {
                BankCard bankCard = resource.getBankCard().getBankCard();
                destination.setResourceBankCardToken(bankCard.getToken());
                destination.setResourceBankCardBin(bankCard.getBin());
                destination.setResourceBankCardMaskedPan(bankCard.getMaskedPan());
                if (bankCard.isSetPaymentSystem()) {
                    destination.setResourceBankCardPaymentSystem(bankCard.getPaymentSystem().toString());
                }
            } else if (resource.isSetCryptoWallet()) {
                CryptoWallet cryptoWallet = resource.getCryptoWallet().getCryptoWallet();
                destination.setCryptoWalletId(cryptoWallet.getId());
                destination.setCryptoWalletCurrency(cryptoWallet.getData().getSetField().getFieldName());
            }
            destination.setResourceType(TBaseUtil.unionFieldToEnum(resource, DestinationResourceType.class));

            destinationDao.updateNotCurrent(event.getSource());
            destinationDao.save(destination);
            log.info("Destination have been saved, eventId={}, destinationId={}", event.getId(), event.getSource());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
