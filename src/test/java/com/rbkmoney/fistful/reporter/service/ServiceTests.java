package com.rbkmoney.fistful.reporter.service;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.damsel.domain.Party;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.fistful.reporter.config.AbstractServiceConfig;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.service.impl.WithdrawalRegistryTemplateServiceImpl;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static java.nio.file.Files.*;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

public class ServiceTests extends AbstractServiceConfig {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockBean
    private PartyManagementSrv.Iface partyManagementClient;

    @Autowired
    private ReportService reportService;

    @Autowired
    private PartyManagementService partyManagementService;

    @Autowired
    private WithdrawalRegistryTemplateServiceImpl withdrawalRegistryTemplateService;

    @Autowired
    private WithdrawalService withdrawalService;

    @Test
    public void partyManagementServiceTest() throws TException {
        Contract contract = new Contract();
        contract.setId("0");
        Party party = new Party();
        party.setId("0");

        party.setContracts(Map.of(contract.getId(), contract));
        UserInfo userInfo = new UserInfo("fistful-reporter", UserType.internal_user(new InternalUser()));
        PartyRevisionParam revision = PartyRevisionParam.revision(0L);

        Mockito.when(partyManagementClient.getContract(userInfo, party.getId(), contract.getId()))
                .thenReturn(contract);
        Mockito.when(partyManagementClient.checkout(userInfo, party.getId(), revision))
                .thenReturn(party);

        Contract c = partyManagementService.getContract(party.getId(), contract.getId());
        Assert.assertEquals(contract, c);
        Party p = partyManagementService.getParty(party.getId(), revision);
        Assert.assertEquals(party, p);
    }

    @Test
    public void reportServiceTest() {
        jdbcTemplate.execute("truncate table fr.report cascade");

        List<Long> reportIds = range(0, 5)
                .mapToLong(i -> createReport("withdrawalRegistry"))
                .boxed()
                .collect(toList());
        reportIds.add(createReport("anotherReportType"));

        assertEquals(
                5,
                reportService.getReportsByRange(
                        partyId,
                        contractId,
                        getFromTime().toInstant(ZoneOffset.UTC),
                        getToTime().toInstant(ZoneOffset.UTC),
                        singletonList("withdrawalRegistry")
                )
                        .size()
        );

        Long reportId = reportIds.get(0);
        Report report = reportService.getReport(partyId, contractId, reportId);
        assertEquals(getToTime(), report.getToTime());

        reportService.cancelReport(partyId, contractId, reportId);

        assertEquals(
                4,
                reportService.getReportsByRangeNotCancelled(
                        partyId,
                        contractId,
                        getFromTime().toInstant(ZoneOffset.UTC),
                        getToTime().toInstant(ZoneOffset.UTC),
                        singletonList("withdrawalRegistry")
                )
                        .size()
        );

        reportService.changeReportStatus(report, ReportStatus.created);

        assertEquals(
                5,
                reportService.getReportsByRangeNotCancelled(
                        partyId,
                        contractId,
                        getFromTime().toInstant(ZoneOffset.UTC),
                        getToTime().toInstant(ZoneOffset.UTC),
                        singletonList("withdrawalRegistry")
                )
                        .size()
        );

        assertEquals(5, reportService.getPendingReports().size());
    }

    @Test
    public void withdrawalRegistryTemplateServiceTest() throws IOException {
        report.setTimezone("Europe/Moscow");
        Path reportFile = createTempFile(report.getType() + "_", "_report.xlsx");
        try {
            withdrawalRegistryTemplateService.processReportFileByTemplate(report, newOutputStream(reportFile));
        } finally {
            deleteIfExists(reportFile);
        }
    }

    @Test
    public void withdrawalServiceTest() {
        saveWithdrawalsDependencies();

        List<Withdrawal> withdrawals = withdrawalService.getSucceededWithdrawalsByReport(report);

        assertEquals(getExpectedSize(), withdrawals.size());
    }

    @Override
    protected int getExpectedSize() {
        return 2000;
    }

    private long createReport(String reportType) {
        return reportService.createReport(
                partyId,
                contractId,
                getFromTime().toInstant(ZoneOffset.UTC),
                getToTime().toInstant(ZoneOffset.UTC),
                reportType
        );
    }
}
