package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.dao.GenericDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Challenge;

public interface ChallengeDao extends GenericDao {

    Long save(Challenge challenge);

    Challenge get(String identityId, String challengeId);

    void updateNotCurrent(String identityId, String challengeId);

}
