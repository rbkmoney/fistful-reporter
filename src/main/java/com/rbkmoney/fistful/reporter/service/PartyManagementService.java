package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.damsel.domain.Party;
import com.rbkmoney.damsel.payment_processing.PartyRevisionParam;

public interface PartyManagementService {

    Party getParty(String partyId, PartyRevisionParam partyRevisionParam);

    Contract getContract(String partyId, String contractId);

}
