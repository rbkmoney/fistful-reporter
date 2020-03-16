package com.rbkmoney.fistful.reporter.dao;

import com.rbkmoney.fistful.reporter.config.AbstractDaoConfig;
import com.rbkmoney.fistful.reporter.domain.enums.FistfulCashFlowChangeType;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.*;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDateTime;
import java.util.List;

import static io.github.benas.randombeans.api.EnhancedRandom.random;
import static io.github.benas.randombeans.api.EnhancedRandom.randomListOf;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DaoTests extends AbstractDaoConfig {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ChallengeDao challengeDao;

    @Autowired
    private DepositDao depositDao;

    @Autowired
    private DestinationDao destinationDao;

    @Autowired
    private FileInfoDao fileInfoDao;

    @Autowired
    private ReportDao reportDao;

    @Autowired
    private FistfulCashFlowDao fistfulCashFlowDao;

    @Autowired
    private SourceDao sourceDao;

    @Test
    public void challengeDaoTest() {
        Challenge challenge = random(Challenge.class);
        challenge.setCurrent(true);
        Long id = challengeDao.save(challenge);
        challenge.setId(id);
        assertEquals(challenge, challengeDao.get(challenge.getIdentityId(), challenge.getChallengeId()));
        challengeDao.updateNotCurrent(challenge.getIdentityId(), challenge.getChallengeId());
        assertNull(challengeDao.get(challenge.getIdentityId(), challenge.getChallengeId()));
    }

    @Test
    public void depositDaoTest() {
        Deposit deposit = random(Deposit.class);
        deposit.setCurrent(true);
        Long id = depositDao.save(deposit);
        deposit.setId(id);
        assertEquals(deposit, depositDao.get(deposit.getDepositId()));
        depositDao.updateNotCurrent(deposit.getDepositId());
        assertNull(depositDao.get(deposit.getDepositId()));
    }

    public void depositDaoDuplicationTest() {
        Deposit deposit = random(Deposit.class);
        deposit.setCurrent(true);
        depositDao.updateNotCurrent(deposit.getDepositId());
        depositDao.save(deposit);
        depositDao.updateNotCurrent(deposit.getDepositId());
        Long id = depositDao.save(deposit);
        deposit.setId(id);
        assertEquals(deposit, depositDao.get(deposit.getDepositId()));
        depositDao.updateNotCurrent(deposit.getDepositId());
        assertNull(depositDao.get(deposit.getDepositId()));
    }

    @Test
    public void destinationDaoTest() {
        Destination destination = random(Destination.class);
        destination.setCurrent(true);
        Long id = destinationDao.save(destination);
        destination.setId(id);
        assertEquals(destination, destinationDao.get(destination.getDestinationId()));
        destinationDao.updateNotCurrent(destination.getDestinationId());
        assertNull(destinationDao.get(destination.getDestinationId()));
    }

    @Test
    public void fileInfoDaoTest() {
        final int size = 4;
        long reportId;
        Report report = random(Report.class);
        reportId = reportDao.save(report);
        List<FileInfo> fileInfos = randomListOf(size, FileInfo.class);
        for (FileInfo fileInfo : fileInfos) {
            fileInfo.setReportId(reportId);
            fileInfoDao.save(fileInfo);

        }
        assertEquals(size, fileInfoDao.getByReportId(reportId).size());
    }

    @Test
    public void fistfulCashFlowDaoTest() {
        FistfulCashFlowChangeType objType = FistfulCashFlowChangeType.deposit;
        int size = 4;
        long objId = generateLong();
        List<FistfulCashFlow> fistfulCashFlows = randomListOf(size, FistfulCashFlow.class);
        for (FistfulCashFlow fistfulCashFlow : fistfulCashFlows) {
            fistfulCashFlow.setObjId(objId);
            fistfulCashFlow.setObjType(objType);
        }
        fistfulCashFlowDao.save(fistfulCashFlows);
        assertEquals(size, fistfulCashFlowDao.getByObjId(objId, objType).size());
    }

    @Test
    public void identityDaoTest() {
        Identity identity = random(Identity.class);
        identity.setCurrent(true);
        Long id = identityDao.save(identity);
        identity.setId(id);
        assertEquals(identity, identityDao.get(identity.getIdentityId()));
        identityDao.updateNotCurrent(identity.getIdentityId());
        assertNull(identityDao.get(identity.getIdentityId()));
    }

    @Test
    public void reportDaoTest() {
        jdbcTemplate.execute("truncate table fr.report cascade");

        Report report = random(Report.class);
        configReport(report, getFromTime(), getToTime());
        long id = reportDao.save(report);
        Report expectedReport = reportDao.getReport(id, report.getPartyId(), report.getContractId());
        assertEquals(report, expectedReport);

        List<Report> reports = randomListOf(4, Report.class);
        for (Report r : reports) {
            configReport(r, getFromTime(), getToTime());
            reportDao.save(r);
        }
        assertEquals(5, reportDao.getPendingReports().size());

        reportDao.changeReportStatus(id, ReportStatus.created);
        assertEquals(4, reportDao.getPendingReports().size());

        assertEquals(5, reportDao.getReportsByRange(partyId, contractId, getFromTime(), getToTime(), emptyList()).size());

        assertEquals(0, reportDao.getReportsByRange(partyId, contractId, getFromTime().plusMinutes(1), getToTime(), emptyList()).size());
    }

    @Test
    public void sourceDaoTest() {
        Source source = random(Source.class);
        source.setCurrent(true);
        Long id = sourceDao.save(source);
        source.setId(id);
        assertEquals(source, sourceDao.get(source.getSourceId()));
        sourceDao.updateNotCurrent(source.getSourceId());
        assertNull(sourceDao.get(source.getSourceId()));
    }

    @Test
    public void walletDaoTest() {
        Wallet wallet = random(Wallet.class);
        wallet.setCurrent(true);
        Long id = walletDao.save(wallet);
        wallet.setId(id);
        assertEquals(wallet, walletDao.get(wallet.getWalletId()));
        walletDao.updateNotCurrent(wallet.getWalletId());
        assertNull(walletDao.get(wallet.getWalletId()));
    }

    @Test
    public void withdrawalDaoTest() {
        Withdrawal withdrawal = random(Withdrawal.class);
        withdrawal.setCurrent(true);
        Long id = withdrawalDao.save(withdrawal);
        withdrawal.setId(id);
        assertEquals(withdrawal, withdrawalDao.get(withdrawal.getWithdrawalId()));
        withdrawalDao.updateNotCurrent(withdrawal.getWithdrawalId());
        assertNull(withdrawalDao.get(withdrawal.getWithdrawalId()));
    }

    @Test
    public void takeSucceededWithdrawalsTest() {
        saveWithdrawalsDependencies();
        List<Withdrawal> withdrawalsByReport = withdrawalDao.getSucceededWithdrawalsByReport(report, 0L, 1000);
        assertEquals(getExpectedSize(), withdrawalsByReport.size());
    }

    @Override
    protected int getExpectedSize() {
        return 20;
    }

    private void configReport(Report r, LocalDateTime fromTime, LocalDateTime toTime) {
        r.setStatus(ReportStatus.pending);
        r.setFromTime(fromTime);
        r.setToTime(toTime);
        r.setPartyId(partyId);
        r.setContractId(contractId);
    }
}
