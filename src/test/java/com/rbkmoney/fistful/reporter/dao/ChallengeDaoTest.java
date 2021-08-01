package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Challenge;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static com.rbkmoney.testcontainers.annotations.util.RandomBeans.random;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@PostgresqlSpringBootITest
public class ChallengeDaoTest {

    @Autowired
    private ChallengeDao challengeDao;

    @Test
    public void challengeDaoTest() {
        Challenge challenge = random(Challenge.class);
        challenge.setCurrent(true);
        Long id = challengeDao.save(challenge).get();
        challenge.setId(id);
        assertEquals(challenge, challengeDao.get(challenge.getIdentityId(), challenge.getChallengeId()));
        challengeDao.updateNotCurrent(challenge.getId());
        assertNull(challengeDao.get(challenge.getIdentityId(), challenge.getChallengeId()));
    }
}
