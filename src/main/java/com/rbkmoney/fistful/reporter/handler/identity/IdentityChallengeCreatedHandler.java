package com.rbkmoney.fistful.reporter.handler.identity;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.identity.ChallengeChange;
import com.rbkmoney.fistful.identity.ChallengeChangePayload;
import com.rbkmoney.fistful.identity.TimestampedChange;
import com.rbkmoney.fistful.reporter.dao.ChallengeDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.ChallengeEventType;
import com.rbkmoney.fistful.reporter.domain.enums.ChallengeStatus;
import com.rbkmoney.fistful.reporter.domain.enums.IdentityEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Challenge;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityChallengeCreatedHandler implements IdentityEventHandler {

    private final ChallengeDao challengeDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetIdentityChallenge()
                && change.getChange().getIdentityChallenge().getPayload().isSetCreated();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            log.info("Start identity challenge created handling, eventId={}, identityId={}", event.getEventId(), event.getSourceId());
            ChallengeChange challengeChange = saveChallenge(change, event);

            log.info("Challenge created handling: start update identity, eventId={}, identityId={}", event.getEventId(), event.getSourceId());
            updateIdentity(event, change);
            log.info("Challenge created handling: identity have been updated, eventId={}, identityId={}", event.getEventId(), event.getSourceId());

            log.info("Start identity challenge have been created, eventId={}, identityId={}, challengeId={}", event.getEventId(), event.getSourceId(), challengeChange.getId());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private ChallengeChange saveChallenge(TimestampedChange change, MachineEvent event) {
        ChallengeChange challengeChange = change.getChange().getIdentityChallenge();

        Challenge challenge = new Challenge();

        challenge.setEventId(event.getEventId());
        challenge.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        challenge.setIdentityId(event.getSourceId());
        challenge.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        challenge.setEventType(ChallengeEventType.CHALLENGE_CREATED);
        challenge.setChallengeId(challengeChange.getId());

        ChallengeChangePayload challengePayload = challengeChange.getPayload();
        challenge.setChallengeClassId(challengePayload.getCreated().getCls());
        challenge.setChallengeStatus(ChallengeStatus.pending);

        challengeDao.updateNotCurrent(event.getSourceId(), challengeChange.getId());
        challengeDao.save(challenge);
        return challengeChange;
    }

    private void updateIdentity(MachineEvent event, TimestampedChange change) {
        Identity identity = identityDao.get(event.getSourceId());

        identity.setId(null);
        identity.setWtime(null);

        identity.setEventId(event.getEventId());
        identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        identity.setIdentityId(event.getSourceId());
        identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        identity.setEventType(IdentityEventType.IDENTITY_CHALLENGE_CREATED);

        identityDao.updateNotCurrent(event.getSourceId());
        identityDao.save(identity);
    }
}