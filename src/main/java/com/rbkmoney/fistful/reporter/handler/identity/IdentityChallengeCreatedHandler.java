package com.rbkmoney.fistful.reporter.handler.identity;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.identity.*;
import com.rbkmoney.fistful.reporter.dao.ChallengeDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.ChallengeStatus;
import com.rbkmoney.fistful.reporter.domain.enums.*;
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
            log.info("Start identity challenge created handling, eventId={}, identityId={}",
                    event.getEventId(), event.getSourceId());

            log.info("Challenge created handling: start update identity, eventId={}, identityId={}",
                    event.getEventId(), event.getSourceId());
            updateIdentity(event, change);
            log.info("Challenge created handling: identity has been updated, eventId={}, identityId={}",
                    event.getEventId(), event.getSourceId());

            ChallengeChange challengeChange = saveChallenge(change, event);
            log.info("Start identity challenge has been created, eventId={}, identityId={}, challengeId={}",
                    event.getEventId(), event.getSourceId(), challengeChange.getId());
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private ChallengeChange saveChallenge(TimestampedChange change, MachineEvent event) {
        Challenge challenge = new Challenge();
        challenge.setEventId(event.getEventId());
        challenge.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        challenge.setIdentityId(event.getSourceId());
        challenge.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        challenge.setEventType(ChallengeEventType.CHALLENGE_CREATED);
        ChallengeChange challengeChange = change.getChange().getIdentityChallenge();
        challenge.setChallengeId(challengeChange.getId());
        ChallengeChangePayload challengePayload = challengeChange.getPayload();
        challenge.setChallengeClassId(challengePayload.getCreated().getCls());
        challenge.setChallengeStatus(ChallengeStatus.pending);

        challengeDao.save(challenge).ifPresentOrElse(
                id -> log.info("Start identity challenge have been changed,  eventId={}, identityId={}",
                        event.getEventId(), event.getSourceId()),
                () -> log.info("Identity challenge have been saved, eventId={}, identityId={}",
                        event.getEventId(), event.getSourceId())
        );
        return challengeChange;
    }

    private void updateIdentity(MachineEvent event, TimestampedChange change) {
        Identity oldIdentity = identityDao.get(event.getSourceId());
        Identity updatedIdentity = update(oldIdentity, change, event);
        identityDao.save(updatedIdentity).ifPresentOrElse(
                id -> {
                    identityDao.updateNotCurrent(oldIdentity.getId());
                    log.info("Start identity challenge have been updated, eventId={}, identityId={}",
                            event.getEventId(), event.getSourceId());
                },
                () -> log.info("Identity challenge bound duplicated, eventId={}, identityId={}",
                        event.getEventId(), event.getSourceId())
        );
    }

    private Identity update(
            Identity oldIdentity,
            TimestampedChange change,
            MachineEvent event) {
        Identity identity = new Identity(oldIdentity);
        identity.setId(null);
        identity.setWtime(null);
        identity.setEventId(event.getEventId());
        identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        identity.setIdentityId(event.getSourceId());
        identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        identity.setEventType(IdentityEventType.IDENTITY_CHALLENGE_CREATED);
        return identity;
    }
}
