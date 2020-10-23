package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.fistful.reporter.*;
import com.rbkmoney.fistful.reporter.config.AbstractHandlerConfig;
import com.rbkmoney.fistful.reporter.generator.ReportGenerator;
import com.rbkmoney.fistful.reporter.service.FileStorageService;
import com.rbkmoney.fistful.reporter.service.PartyManagementService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static com.rbkmoney.geck.common.util.TypeUtil.temporalToString;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class HandlerTest extends AbstractHandlerConfig {

    @MockBean
    private PartyManagementService partyManagementService;

    @MockBean
    private FileStorageService fileStorageService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ReportService reportService;

    @Autowired
    private ReportGenerator reportGenerator;

    @Autowired
    private ReportingSrv.Iface reportHandler;

    private ReportRequest request;

    @Before
    public void setUp() {
        ReportTimeRange reportTimeRange = new ReportTimeRange(
                temporalToString(getFromTime()),
                temporalToString(getToTime())
        );
        request = new ReportRequest(partyId, contractId, reportTimeRange);
    }

    @Test(expected = InvalidRequest.class)
    public void exceptionArgTest() throws TException {
        reportHandler.generateReport(request, "kek");
    }

    @Test
    public void fistfulReporterTest() throws TException, IOException {
        jdbcTemplate.execute("truncate table fr.report cascade");

        when(partyManagementService.getContract(anyString(), anyString())).thenReturn(new Contract());
        when(fileStorageService.saveFile(any())).thenReturn(UUID.randomUUID().toString());

        saveWithdrawalsDependencies();

        long reportId = reportHandler.generateReport(request, "withdrawalRegistry");

        schedulerEmulation();

        Report report = reportHandler.getReport(partyId, contractId, reportId);

        assertEquals(ReportStatus.created, report.getStatus());
        assertEquals(1, report.getFileDataIds().size());
        verify(fileStorageService, times(1)).saveFile(any());
    }

    @Override
    protected int getExpectedSize() {
        return 5;
    }

    private void schedulerEmulation() {
        var pendingReports = reportService.getFirstPendingReport();
        reportGenerator.generateReportFile(pendingReports);
    }
}
