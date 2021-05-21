package com.rbkmoney.fistful.reporter.handler.destination;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.base.BankCard;
import com.rbkmoney.fistful.base.CryptoWallet;
import com.rbkmoney.fistful.base.Resource;
import com.rbkmoney.fistful.destination.TimestampedChange;
import com.rbkmoney.fistful.reporter.dao.DestinationDao;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationEventType;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationResourceType;
import com.rbkmoney.fistful.reporter.domain.enums.DestinationStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Destination;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class DestinationCreatedHandler implements DestinationEventHandler {

    private final DestinationDao destinationDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start destination created handling, eventId={}, destinationId={}",
                    event.getEventId(), event.getSourceId());
            Destination destination = new Destination();
            destination.setEventId(event.getEventId());
            destination.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
            destination.setDestinationId(event.getSourceId());
            destination.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
            destination.setEventType(DestinationEventType.DESTINATION_CREATED);
            destination.setDestinationName(change.getChange().getCreated().getName());
            destination.setDestinationStatus(DestinationStatus.unauthorized);

            Resource resource = change.getChange().getCreated().getResource();
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

            destinationDao.save(destination).ifPresentOrElse(
                    dbContractId -> log.info("Destination created has been saved, eventId={}, destinationId={}",
                            event.getEventId(), event.getSourceId()),
                    () -> log.info("Destination created bound duplicated, eventId={}, destinationId={}",
                            event.getEventId(), event.getSourceId()));
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }
}
