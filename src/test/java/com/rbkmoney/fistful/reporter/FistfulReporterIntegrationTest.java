package com.rbkmoney.fistful.reporter;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.file.storage.FileStorageSrv;
import com.rbkmoney.fistful.reporter.component.ReportGenerator;
import com.rbkmoney.fistful.reporter.exception.DaoException;
import com.rbkmoney.fistful.reporter.service.PartyManagementService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import com.rbkmoney.fistful.reporter.service.impl.WithdrawalRegistryTemplateServiceImpl;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.thrift.TException;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.rbkmoney.geck.common.util.TypeUtil.temporalToString;
import static java.nio.file.Files.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

public class FistfulReporterIntegrationTest extends AbstractAppFistfulReporterIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockBean
    private PartyManagementService partyManagementService;

    @Autowired
    private HttpClient httpClient;

    @Autowired
    private ReportService reportService;

    @Autowired
    private WithdrawalRegistryTemplateServiceImpl withdrawalRegistryTemplateService;

    @Autowired
    private ReportGenerator reportGenerator;

    @Autowired
    private FileStorageSrv.Iface fileStorageClient;

    @Autowired
    private ReportingSrv.Iface reportClient;

    private ReportTimeRange reportTimeRange = new ReportTimeRange(
            temporalToString(fromTime),
            temporalToString(toTime)
    );
    private ReportRequest request = new ReportRequest(partyId, contractId, reportTimeRange);

    @Test(expected = InvalidRequest.class)
    public void exceptionArgTest() throws TException {
        reportClient.generateReport(request, "kek");
    }

    @Test
    public void fistfulReporterTest() throws TException, IOException, DaoException {
        jdbcTemplate.execute("truncate table fr.report cascade");

        when(partyManagementService.getContract(anyString(), anyString())).thenReturn(new Contract());
        saveWithdrawalsDependencies();
        try {
            long reportId = reportClient.generateReport(request, "withdrawalRegistry");
            prepareForMainAssert();
            serverSideInitLogic();
            clientSideLogic(reportId);
        } finally {
            deleteIfExists(reportFile);
        }
    }

    @Override
    protected int getExpectedSize() {
        return 5;
    }

    private void prepareForMainAssert() throws IOException {
        report.setTimezone("Europe/Moscow");
        withdrawalRegistryTemplateService.processReportFileByTemplate(report, newOutputStream(reportFile));
    }

    private void serverSideInitLogic() {
        List<com.rbkmoney.fistful.reporter.domain.tables.pojos.Report> pendingReports = reportService.getPendingReports();
        assertEquals(1, pendingReports.size());
        reportGenerator.generateReportFile(pendingReports.get(0));
    }

    private void clientSideLogic(long reportId) throws TException, IOException {
        Report report = reportClient.getReport(partyId, contractId, reportId);
        assertEquals(ReportStatus.created, report.getStatus());
        assertEquals(1, report.getFileDataIds().size());
        String downloadUrl = fileStorageClient.generateDownloadUrl(
                report.getFileDataIds().get(0),
                generateCurrentTimePlusDay().toString()
        );

        HttpResponse responseGet = httpClient.execute(new HttpGet(downloadUrl));
        InputStream content = responseGet.getEntity().getContent();
        assertEquals(getContent(newInputStream(reportFile)).substring(0, 5), getContent(content).substring(0, 5));
    }
}
