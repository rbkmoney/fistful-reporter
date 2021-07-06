package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.config.AbstractHandlerConfig;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.handler.wallet.WalletAccountCreatedHandler;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Wallet.WALLET;
import static com.rbkmoney.fistful.reporter.utils.handler.WalletHandlerTestUtils.createAccountCreated;
import static com.rbkmoney.fistful.reporter.utils.handler.WalletHandlerTestUtils.createMachineEvent;
import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static org.junit.Assert.assertEquals;

public class WalletHandlerTest extends AbstractHandlerConfig {

    @Autowired
    private WalletAccountCreatedHandler walletAccountCreatedHandler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    Wallet wallet = random(Wallet.class);
    Identity identity = random(Identity.class);
    Account account = random(Account.class);
    String sqlStatement = "select * from fr.wallet where id='" + wallet.getId() + "';";

    @Test
    public void walletAccountCreatedHandlerTest() {
        wallet.setCurrent(true);
        walletDao.save(wallet);
        identity.setIdentityId(wallet.getIdentityId());
        identity.setCurrent(true);
        identityDao.save(identity);
        account.setIdentity(wallet.getIdentityId());
        walletAccountCreatedHandler.handle(
                createAccountCreated(account),
                createMachineEvent(wallet.getWalletId(), account));
        assertEquals(2L, walletDao.get(wallet.getWalletId()).getEventId().longValue());
        assertEquals(
                false,
                jdbcTemplate.queryForObject(sqlStatement,
                        new RecordRowMapper<>(WALLET, Wallet.class)).getCurrent()
        );
    }


    @Override
    protected int getExpectedSize() {
        return 0;
    }
}
