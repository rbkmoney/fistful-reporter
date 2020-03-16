package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.fistful.reporter.config.AbstractEventServiceConfig;
import com.rbkmoney.fistful.reporter.dao.*;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.*;
import com.rbkmoney.fistful.reporter.service.impl.*;
import com.rbkmoney.fistful.reporter.utils.*;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static io.github.benas.randombeans.api.EnhancedRandom.randomListOf;
import static org.mockito.Mockito.*;

public class EventServiceTests extends AbstractEventServiceConfig {

    @MockBean
    private IdentityDao identityDao;

    @MockBean
    private WalletDao walletDao;

    @MockBean
    private DepositDao depositDao;

    @MockBean
    private DestinationDao destinationDao;

    @MockBean
    private FistfulCashFlowDao fistfulCashFlowDao;

    @MockBean
    private ChallengeDao challengeDao;

    @MockBean
    private SourceDao sourceDao;

    @MockBean
    private WithdrawalDao withdrawalDao;

    @Autowired
    private DepositEventService depositEventService;

    @Autowired
    private DestinationEventService destinationEventService;

    @Autowired
    private IdentityEventService identityEventService;

    @Autowired
    private SourceEventService sourceEventService;

    @Autowired
    private WalletEventService walletEventService;

    @Autowired
    private WithdrawalEventService withdrawalEventService;

    @Test
    public void depositEventServiceTest() {
        generateAndMockCashFlow();
        String walletId = generateAndMockWallet();
        String depositId = generateAndMockDeposit();

        depositEventService.processSinkEvent(DepositSinkEventTestUtils.create(depositId, walletId));

        verify(depositDao, times(4)).save(any());
        verify(fistfulCashFlowDao, times(3)).save(anyList());
    }

    @Test
    public void destinationEventServiceTest() {
        String identityId = generateAndMockIdentity();
        String destinationId = generateAndMockDestination();

        destinationEventService.processSinkEvent(DestinationSinkEventTestUtils.create(destinationId, identityId));

        verify(destinationDao, times(3)).save(any());
    }

    @Test
    public void identityEventServiceTest() {
        generateAndMockChallenge();
        String identityId = generateAndMockIdentity();

        identityEventService.processSinkEvent(IdentitySinkEventTestUtils.create(identityId));

        verify(identityDao, times(5)).save(any());
    }

    @Test
    public void sourceEventServiceTest() {
        String identityId = generateAndMockIdentity();
        String sourceId = generateAndMockSource();

        sourceEventService.processSinkEvent(SourceSinkEventTestUtils.create(sourceId, identityId));

        verify(sourceDao, times(3)).save(any());
    }

    @Test
    public void walletEventServiceTest() {
        String identityId = generateAndMockIdentity();
        String walletId = generateAndMockWallet();

        walletEventService.processSinkEvent(WalletSinkEventTestUtils.test(walletId, identityId));

        verify(walletDao, times(2)).save(any());
    }

    @Test
    public void withdrawalEventServiceTest() {
        String walletId = generateAndMockWallet();
        String withdrawalId = generateAndMockWithdrawal();

        withdrawalEventService.processSinkEvent(WithdrawalSinkEventTestUtils.create(withdrawalId, walletId));

        verify(withdrawalDao, times(5)).save(any());
    }

    private String generateAndMockDestination() {
        Destination destination = random(Destination.class);
        destination.setId(null);
        destination.setCurrent(true);
        reset(destinationDao);
        when(destinationDao.get(anyString())).thenReturn(destination);
        return destination.getDestinationId();
    }

    private String generateAndMockDeposit() {
        Deposit deposit = random(Deposit.class);
        deposit.setId(null);
        deposit.setCurrent(true);
        reset(depositDao);
        when(depositDao.get(anyString())).thenReturn(deposit);
        return deposit.getDepositId();
    }

    private String generateAndMockIdentity() {
        Identity identity = random(Identity.class);
        identity.setId(null);
        identity.setCurrent(true);
        reset(identityDao);
        when(identityDao.get(anyString())).thenReturn(identity);
        return identity.getIdentityId();
    }

    private String generateAndMockWallet() {
        Wallet wallet = random(Wallet.class);
        wallet.setId(null);
        wallet.setCurrent(true);
        reset(walletDao);
        when(walletDao.get(anyString())).thenReturn(wallet);
        return wallet.getWalletId();
    }

    private String generateAndMockChallenge() {
        Challenge challenge = random(Challenge.class);
        challenge.setId(null);
        challenge.setCurrent(true);
        reset(challengeDao);
        when(challengeDao.get(anyString(), anyString())).thenReturn(challenge);
        return challenge.getChallengeId();
    }

    private String generateAndMockSource() {
        Source source = random(Source.class);
        source.setId(null);
        source.setCurrent(true);
        reset(sourceDao);
        when(sourceDao.get(anyString())).thenReturn(source);
        return source.getSourceId();
    }

    private String generateAndMockWithdrawal() {
        Withdrawal withdrawal = random(Withdrawal.class);
        withdrawal.setId(null);
        withdrawal.setCurrent(true);
        reset(withdrawalDao);
        when(withdrawalDao.get(anyString())).thenReturn(withdrawal);
        return withdrawal.getWithdrawalId();
    }

    private void generateAndMockCashFlow() {
        List<FistfulCashFlow> fistfulCashFlows = randomListOf(1, FistfulCashFlow.class);
        reset(fistfulCashFlowDao);
        when(fistfulCashFlowDao.getByObjId(anyLong(), any())).thenReturn(fistfulCashFlows);
    }
}
