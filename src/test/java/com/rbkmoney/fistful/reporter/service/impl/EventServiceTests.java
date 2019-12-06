package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.*;
import com.rbkmoney.fistful.reporter.utils.*;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class EventServiceTests extends AbstractAppEventServiceTests {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private IdentityDao identityDao;

    @Autowired
    private WalletDao walletDao;

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
        String walletId = generateAndSaveWallet();

        String depositId = generateString();
        depositEventService.processSinkEvent(DepositSinkEventTestUtils.create(depositId, walletId));

        List<Deposit> deposits = jdbcTemplate.query(
                "SELECT * FROM fr.deposit AS deposit WHERE deposit.deposit_id = ?",
                new Object[]{depositId},
                new BeanPropertyRowMapper<>(Deposit.class)
        );
        assertEquals(4, deposits.size());

        deposits = jdbcTemplate.query(
                "SELECT * FROM fr.deposit AS deposit WHERE deposit.deposit_id = ? AND deposit.current",
                new Object[]{depositId},
                new BeanPropertyRowMapper<>(Deposit.class)
        );
        assertEquals(1, deposits.size());
    }

    @Test
    public void destinationEventServiceTest() {
        String identityId = generateAndSaveIdentity();

        String destinationId = generateString();
        destinationEventService.processSinkEvent(DestinationSinkEventTestUtils.create(destinationId, identityId));

        List<Destination> deposits = jdbcTemplate.query(
                "SELECT * FROM fr.destination AS destination WHERE destination.destination_id = ?",
                new Object[]{destinationId},
                new BeanPropertyRowMapper<>(Destination.class)
        );
        assertEquals(3, deposits.size());

        deposits = jdbcTemplate.query(
                "SELECT * FROM fr.destination AS destination WHERE destination.destination_id = ? AND destination.current",
                new Object[]{destinationId},
                new BeanPropertyRowMapper<>(Destination.class)
        );
        assertEquals(1, deposits.size());
    }

    @Test
    public void identityEventServiceTest() {
        String identityId = generateString();
        identityEventService.processSinkEvent(IdentitySinkEventTestUtils.create(identityId));

        List<Identity> identities = jdbcTemplate.query(
                "SELECT * FROM fr.identity AS identity WHERE identity.identity_id = ?",
                new Object[]{identityId},
                new BeanPropertyRowMapper<>(Identity.class)
        );
        assertEquals(5, identities.size());

        identities = jdbcTemplate.query(
                "SELECT * FROM fr.identity AS identity WHERE identity.identity_id = ? AND identity.current",
                new Object[]{identityId},
                new BeanPropertyRowMapper<>(Identity.class)
        );
        assertEquals(1, identities.size());
    }

    @Test
    public void sourceEventServiceTest() {
        String identityId = generateAndSaveIdentity();

        String sourceId = generateString();
        sourceEventService.processSinkEvent(SourceSinkEventTestUtils.create(sourceId, identityId));

        List<Source> sources = jdbcTemplate.query(
                "SELECT * FROM fr.source AS source WHERE source.source_id = ?",
                new Object[]{sourceId},
                new BeanPropertyRowMapper<>(Source.class)
        );
        assertEquals(3, sources.size());

        sources = jdbcTemplate.query(
                "SELECT * FROM fr.source AS source WHERE source.source_id = ? AND source.current",
                new Object[]{sourceId},
                new BeanPropertyRowMapper<>(Source.class)
        );
        assertEquals(1, sources.size());
    }

    @Test
    public void walletEventServiceTest() {
        String identityId = generateAndSaveIdentity();

        String walletId = generateString();
        walletEventService.processSinkEvent(WalletSinkEventTestUtils.test(walletId, identityId));

        List<Wallet> wallets = jdbcTemplate.query(
                "SELECT * FROM fr.wallet AS wallet WHERE wallet.wallet_id = ?",
                new Object[]{walletId},
                new BeanPropertyRowMapper<>(Wallet.class)
        );
        assertEquals(2, wallets.size());

        wallets = jdbcTemplate.query(
                "SELECT * FROM fr.wallet AS wallet WHERE wallet.wallet_id = ? AND wallet.current",
                new Object[]{walletId},
                new BeanPropertyRowMapper<>(Wallet.class)
        );
        assertEquals(1, wallets.size());
    }

    @Test
    public void withdrawalEventServiceTest() {
        String walletId = generateAndSaveWallet();

        String withdrawalId = generateString();
        withdrawalEventService.processSinkEvent(WithdrawalSinkEventTestUtils.create(withdrawalId, walletId));

        List<Withdrawal> withdrawals = jdbcTemplate.query(
                "SELECT * FROM fr.withdrawal AS withdrawal WHERE withdrawal.withdrawal_id = ?",
                new Object[]{withdrawalId},
                new BeanPropertyRowMapper<>(Withdrawal.class)
        );
        assertEquals(5, withdrawals.size());

        withdrawals = jdbcTemplate.query(
                "SELECT * FROM fr.withdrawal AS withdrawal WHERE withdrawal.withdrawal_id = ? AND withdrawal.current",
                new Object[]{withdrawalId},
                new BeanPropertyRowMapper<>(Withdrawal.class)
        );
        assertEquals(1, withdrawals.size());
    }

    private String generateAndSaveIdentity() {
        String identityId = generateString();
        Identity identity = random(Identity.class);
        identity.setId(null);
        identity.setIdentityId(identityId);
        identity.setCurrent(true);
        identityDao.save(identity);
        return identityId;
    }

    private String generateAndSaveWallet() {
        String walletId = generateString();
        Wallet wallet = random(Wallet.class);
        wallet.setId(null);
        wallet.setWalletId(walletId);
        wallet.setCurrent(true);
        walletDao.save(wallet);
        return walletId;
    }
}
