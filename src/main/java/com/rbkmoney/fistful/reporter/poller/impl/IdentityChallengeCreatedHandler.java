package com.rbkmoney.fistful.reporter.poller.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.identity.ChallengeChange;
import com.rbkmoney.fistful.identity.ChallengeChangePayload;
import com.rbkmoney.fistful.identity.Change;
import com.rbkmoney.fistful.identity.SinkEvent;
import com.rbkmoney.fistful.reporter.dao.ChallengeDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.ChallengeEventType;
import com.rbkmoney.fistful.reporter.domain.enums.ChallengeStatus;
import com.rbkmoney.fistful.reporter.domain.enums.IdentityEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Challenge;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.poller.IdentityEventHandler;
import com.rbkmoney.geck.common.util.TypeUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class IdentityChallengeCreatedHandler implements IdentityEventHandler {

    private final ChallengeDao challengeDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(Change change) {
        return change.isSetIdentityChallenge()
                && change.getIdentityChallenge().isSetPayload()
                && change.getIdentityChallenge().getPayload().isSetCreated();
    }

    @Override
    public void handle(Change change, SinkEvent event) {
        try {
            log.info("Start identity challenge created handling, eventId={}, identityId={}", event.getId(), event.getSource());
            ChallengeChange challengeChange = saveChallenge(change, event);

            log.info("Challenge created handling: start update identity, eventId={}, identityId={}", event.getId(), event.getSource());
            updateIdentity(event);
            log.info("Challenge created handling: identity have been updated, eventId={}, identityId={}", event.getId(), event.getSource());

            log.info("Start identity challenge have been created, eventId={}, identityId={}, challengeId={}", event.getId(), event.getSource(), challengeChange.getId());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private ChallengeChange saveChallenge(Change change, SinkEvent event) {
        ChallengeChange challengeChange = change.getIdentityChallenge();

        Challenge challenge = new Challenge();

        challenge.setEventId(event.getId());
        challenge.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        challenge.setIdentityId(event.getSource());
        challenge.setSequenceId(event.getPayload().getSequence());
        challenge.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
        challenge.setEventType(ChallengeEventType.CHALLENGE_CREATED);
        challenge.setChallengeId(challengeChange.getId());

        ChallengeChangePayload challengePayload = challengeChange.getPayload();
        challenge.setChallengeClassId(challengePayload.getCreated().getCls());
        challenge.setChallengeStatus(ChallengeStatus.pending);

        challengeDao.updateNotCurrent(event.getSource(), challengeChange.getId());
        challengeDao.save(challenge);
        return challengeChange;
    }

    private void updateIdentity(SinkEvent event) {
        Identity identity = identityDao.get(event.getSource());

        identity.setId(null);
        identity.setWtime(null);

        identity.setEventId(event.getId());
        identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        identity.setIdentityId(event.getSource());
        identity.setSequenceId(event.getPayload().getSequence());
        identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(event.getPayload().getOccuredAt()));
        identity.setEventType(IdentityEventType.IDENTITY_CHALLENGE_CREATED);

        identityDao.updateNotCurrent(event.getSource());
        identityDao.save(identity);
    }
}
