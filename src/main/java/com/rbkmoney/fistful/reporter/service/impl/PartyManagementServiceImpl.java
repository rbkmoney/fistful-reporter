package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.damsel.domain.Party;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.fistful.reporter.exception.ContractNotFoundException;
import com.rbkmoney.fistful.reporter.exception.PartyManagementClientException;
import com.rbkmoney.fistful.reporter.exception.PartyNotFoundException;
import com.rbkmoney.fistful.reporter.service.PartyManagementService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class PartyManagementServiceImpl implements PartyManagementService {

    private final UserInfo userInfo = new UserInfo("fistful-reporter", UserType.internal_user(new InternalUser()));

    private final PartyManagementSrv.Iface partyManagementClient;

    @Override
    public Party getParty(String partyId, PartyRevisionParam partyRevisionParam) {
        log.info("Trying to get party, partyId='{}', partyRevisionParam='{}'", partyId, partyRevisionParam);
        try {
            Party party = partyManagementClient.checkout(userInfo, partyId, partyRevisionParam);
            log.info("Party has been found, partyId='{}', partyRevisionParam='{}'", partyId, partyRevisionParam);
            return party;
        } catch (PartyNotFound ex) {
            throw new PartyNotFoundException(
                    String.format("Party not found, partyId='%s', partyRevisionParam='%s'", partyId, partyRevisionParam), ex
            );
        } catch (InvalidPartyRevision ex) {
            throw new PartyNotFoundException(
                    String.format("Invalid party revision, partyId='%s', partyRevisionParam='%s'", partyId, partyRevisionParam), ex
            );
        } catch (TException ex) {
            throw new PartyManagementClientException(
                    String.format("Failed to get party, partyId='%s', partyRevisionParam='%s'", partyId, partyRevisionParam), ex
            );
        }
    }

    @Override
    public Contract getContract(String partyId, String contractId) {
        log.info("Trying to get contract, partyId='{}', contractId='{}'", partyId, contractId);
        try {
            Contract contract = partyManagementClient.getContract(userInfo, partyId, contractId);
            log.info("Contract has been found, partyId='{}', contractId='{}'", partyId, contractId);
            return contract;
        } catch (PartyNotFound ex) {
            throw new PartyNotFoundException(String.format("Party not found, partyId='%s'", partyId), ex);
        } catch (ContractNotFound ex) {
            throw new ContractNotFoundException(String.format("Contract not found, partyId='%s', contractId='%s'", partyId, contractId));
        } catch (TException ex) {
            throw new PartyManagementClientException(String.format("Failed to get contract, partyId='%s', contractId='%s'", partyId, contractId), ex);
        }
    }
}
