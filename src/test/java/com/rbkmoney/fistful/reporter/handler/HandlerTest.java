package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.fistful.reporter.*;
import com.rbkmoney.fistful.reporter.config.AbstractHandlerConfig;
import com.rbkmoney.fistful.reporter.generator.ReportGenerator;
import com.rbkmoney.fistful.reporter.service.FileStorageService;
import com.rbkmoney.fistful.reporter.service.PartyManagementService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import com.rbkmoney.woody.thrift.impl.http.THSpawnClientBuilder;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static com.rbkmoney.geck.common.util.TypeUtil.temporalToString;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class HandlerTest extends AbstractHandlerConfig {

    private static final int TIMEOUT = 555000;

    @MockBean
    private PartyManagementService partyManagementService;

    @MockBean
    private FileStorageService fileStorageService;

    @Value("${local.server.port}")
    protected int port;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ReportService reportService;

    @Autowired
    private ReportGenerator reportGenerator;

    private ReportingSrv.Iface reportClient;

    private ReportTimeRange reportTimeRange;

    private ReportRequest request;

    @Before
    public void setUp() throws URISyntaxException {
        reportClient = new THSpawnClientBuilder()
                .withAddress(new URI("http://localhost:" + port + "/fistful/reports"))
                .withNetworkTimeout(TIMEOUT)
                .build(ReportingSrv.Iface.class);
        reportTimeRange = new ReportTimeRange(
                temporalToString(getFromTime()),
                temporalToString(getToTime())
        );
        request = new ReportRequest(partyId, contractId, reportTimeRange);
    }

    @Test(expected = InvalidRequest.class)
    public void exceptionArgTest() throws TException {
        reportClient.generateReport(request, "kek");
    }

    @Test
    public void fistfulReporterTest() throws TException, IOException {
        jdbcTemplate.execute("truncate table fr.report cascade");

        when(partyManagementService.getContract(anyString(), anyString())).thenReturn(new Contract());

        saveWithdrawalsDependencies();

        long reportId = reportClient.generateReport(request, "withdrawalRegistry");

        schedulerEmulation();

        Report report = reportClient.getReport(partyId, contractId, reportId);

        assertEquals(ReportStatus.created, report.getStatus());
        assertEquals(1, report.getFileDataIds().size());
        verify(fileStorageService, times(1)).saveFile(any());
    }

    @Override
    protected int getExpectedSize() {
        return 5;
    }

    private void schedulerEmulation() {
        List<com.rbkmoney.fistful.reporter.domain.tables.pojos.Report> pendingReports = reportService.getPendingReports();
        assertEquals(1, pendingReports.size());
        reportGenerator.generateReportFile(pendingReports.get(0));
    }
}
