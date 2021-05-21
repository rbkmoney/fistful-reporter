package com.rbkmoney.fistful.reporter.handler.identity;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.identity.ChallengeChange;
import com.rbkmoney.fistful.identity.ChallengeCompleted;
import com.rbkmoney.fistful.identity.ChallengeStatus;
import com.rbkmoney.fistful.identity.TimestampedChange;
import com.rbkmoney.fistful.reporter.dao.ChallengeDao;
import com.rbkmoney.fistful.reporter.dao.IdentityDao;
import com.rbkmoney.fistful.reporter.domain.enums.ChallengeEventType;
import com.rbkmoney.fistful.reporter.domain.enums.IdentityEventType;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Identity;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.geck.common.util.TBaseUtil;
import com.rbkmoney.geck.common.util.TypeUtil;
import com.rbkmoney.machinegun.eventsink.MachineEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdentityChallengeStatusChangedHandler implements IdentityEventHandler {

    private final ChallengeDao challengeDao;
    private final IdentityDao identityDao;

    @Override
    public boolean accept(TimestampedChange change) {
        return change.getChange().isSetIdentityChallenge()
                && change.getChange().getIdentityChallenge().isSetPayload()
                && change.getChange().getIdentityChallenge().getPayload().isSetStatusChanged();
    }

    @Override
    public void handle(TimestampedChange change, MachineEvent event) {
        try {
            ChallengeChange challengeChange = change.getChange().getIdentityChallenge();
            ChallengeStatus status = challengeChange.getPayload().getStatusChanged();
            log.info("Start identity challenge status changed handling, " +
                    "eventId={}, identityId={}, challengeId={}, status={}",
                    event.getEventId(), event.getSourceId(), challengeChange.getId(), status);

            updateChallenge(event, challengeChange, status, change);
            log.info("Challenge status changed handling: update identity, eventId={}, identityId={}",
                    event.getEventId(), event.getSourceId());

            updateIdentity(event, change);
            log.info("Challenge status changed handling: identity has been updated, eventId={}, identityId={}",
                    event.getEventId(), event.getSourceId());

            log.info("Identity challenge status has been changed, " +
                    "eventId={}, identityId={}, challengeId={}, status={}",
                    event.getEventId(), event.getSourceId(), challengeChange.getId(), status);
        } catch (DaoException e) {
            throw new StorageException(e);
        }
    }

    private void updateChallenge(
            MachineEvent event,
            ChallengeChange challengeChange,
            ChallengeStatus status,
            TimestampedChange change) {
        var challenge = challengeDao.get(event.getSourceId(), challengeChange.getId());

        Long oldId = challenge.getId();

        challenge.setId(null);
        challenge.setWtime(null);
        challenge.setEventId(event.getEventId());
        challenge.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        challenge.setIdentityId(event.getSourceId());
        challenge.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        challenge.setEventType(ChallengeEventType.CHALLENGE_STATUS_CHANGED);
        challenge.setChallengeId(challengeChange.getId());
        challenge.setChallengeStatus(
                TBaseUtil.unionFieldToEnum(status, com.rbkmoney.fistful.reporter.domain.enums.ChallengeStatus.class));
        if (status.isSetCompleted()) {
            ChallengeCompleted challengeCompleted = status.getCompleted();
            challenge.setChallengeResolution(
                    TypeUtil.toEnumField(
                            challengeCompleted.getResolution().toString(),
                            com.rbkmoney.fistful.reporter.domain.enums.ChallengeResolution.class));
            if (challengeCompleted.isSetValidUntil()) {
                challenge.setChallengeValidUntil(TypeUtil.stringToLocalDateTime(challengeCompleted.getValidUntil()));
            }
        }

        challengeDao.save(challenge).ifPresentOrElse(
                id -> {
                    challengeDao.updateNotCurrent(oldId);
                    log.info("Start identity challenge status have been changed,  eventId={}, identityId={}",
                            event.getEventId(), event.getSourceId());
                },
                () -> log.info("Identity challenge status bound duplicated, eventId={}, identityId={}",
                        event.getEventId(), event.getSourceId())
        );
    }

    private void updateIdentity(MachineEvent event, TimestampedChange change) {
        Identity identity = identityDao.get(event.getSourceId());

        Long oldId = identity.getId();

        identity.setId(null);
        identity.setWtime(null);
        identity.setEventId(event.getEventId());
        identity.setEventCreatedAt(TypeUtil.stringToLocalDateTime(event.getCreatedAt()));
        identity.setIdentityId(event.getSourceId());
        identity.setEventOccuredAt(TypeUtil.stringToLocalDateTime(change.getOccuredAt()));
        identity.setEventType(IdentityEventType.IDENTITY_CHALLENGE_STATUS_CHANGED);

        identityDao.save(identity).ifPresentOrElse(
                id -> {
                    identityDao.updateNotCurrent(oldId);
                    log.info("Start identity challenge status have been updated, eventId={}, identityId={}",
                            event.getEventId(), event.getSourceId());
                },
                () -> log.info("Identity challenge status bound duplicated, eventId={}, identityId={}",
                        event.getEventId(), event.getSourceId())
        );
    }
}
