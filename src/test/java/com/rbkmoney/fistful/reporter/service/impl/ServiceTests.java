package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.damsel.domain.Party;
import com.rbkmoney.damsel.payment_processing.*;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.fistful.reporter.domain.enums.ReportStatus;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.service.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static java.lang.String.valueOf;
import static java.nio.file.Files.*;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.assertEquals;

public class ServiceTests extends AbstractAppServiceTests {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private FileInfoService fileInfoService;

    @Autowired
    private ReportService reportService;

    @Autowired
    private FileStorageService fileStorageService;

    @Autowired
    private HttpClient httpClient;

    @Autowired
    private FileStorageSrv.Iface client;

    @Autowired
    private PartyManagementService partyManagementService;

    @MockBean
    private PartyManagementSrv.Iface partyManagementClient;

    @Autowired
    private WithdrawalRegistryTemplateServiceImpl withdrawalRegistryTemplateService;

    @Autowired
    private WithdrawalService withdrawalService;

    @Test
    public void fileInfoServiceTest() {
        long reportId = reportService.createReport(
                partyId,
                contractId,
                getFromTime().toInstant(ZoneOffset.UTC),
                getToTime().toInstant(ZoneOffset.UTC),
                "withdrawalRegistry"
        );

        range(0, 4).forEach(i -> fileInfoService.save(reportId, valueOf(i)));

        assertEquals(4, fileInfoService.getFileDataIds(reportId).size());
        assertEquals(0, fileInfoService.getFileDataIds(reportId + 1).size());
    }

    @Test
    public void fileStorageServiceTest() throws URISyntaxException, IOException, TException, InterruptedException {
        Path file = getFileFromResources();
        String fileDataId = fileStorageService.saveFile(file);
        String downloadUrl = client.generateDownloadUrl(fileDataId, generateCurrentTimePlusDay().toString());

        HttpResponse responseGet = httpClient.execute(new HttpGet(downloadUrl));
        InputStream content = responseGet.getEntity().getContent();
        assertEquals(getContent(newInputStream(file)), getContent(content));
    }

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

    private Path getFileFromResources() throws URISyntaxException {
        ClassLoader classLoader = this.getClass().getClassLoader();

        URL url = requireNonNull(classLoader.getResource("respect"));
        return Paths.get(url.toURI());
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
