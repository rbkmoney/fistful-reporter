package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.fistful.account.Account;
import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.dao.WalletDao;
import com.rbkmoney.fistful.reporter.dao.mapper.RecordRowMapper;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Wallet;
import com.rbkmoney.fistful.reporter.handler.wallet.WalletAccountCreatedHandler;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static com.rbkmoney.fistful.reporter.domain.tables.Wallet.WALLET;
import static com.rbkmoney.fistful.reporter.util.handler.WalletHandlerTestUtil.createAccountCreated;
import static com.rbkmoney.fistful.reporter.util.handler.WalletHandlerTestUtil.createMachineEvent;
import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;

@PostgresqlSpringBootITest
public class WalletHandlerTest {

    @Autowired
    private WalletAccountCreatedHandler walletAccountCreatedHandler;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private WalletDao walletDao;

    @Autowired
    private IdentityDao identityDao;

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
}
