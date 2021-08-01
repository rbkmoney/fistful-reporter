package com.rbkmoney.fistful.reporter.handler;

import com.rbkmoney.damsel.domain.Contract;
import com.rbkmoney.fistful.reporter.*;
import com.rbkmoney.fistful.reporter.config.PostgresqlSpringBootITest;
import com.rbkmoney.fistful.reporter.config.testconfiguration.WithdrawalTestDao;
import com.rbkmoney.fistful.reporter.generator.ReportGenerator;
import com.rbkmoney.fistful.reporter.service.FileStorageService;
import com.rbkmoney.fistful.reporter.service.PartyManagementService;
import com.rbkmoney.fistful.reporter.service.ReportService;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.io.IOException;
import java.util.UUID;

import static com.rbkmoney.fistful.reporter.data.TestData.contractId;
import static com.rbkmoney.fistful.reporter.data.TestData.partyId;
import static com.rbkmoney.geck.common.util.TypeUtil.temporalToString;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getFromTime;
import static com.rbkmoney.testcontainers.annotations.util.ValuesGenerator.getToTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@PostgresqlSpringBootITest
public class HandlerTest {

    @MockBean
    private PartyManagementService partyManagementService;

    @MockBean
    private FileStorageService fileStorageService;

    @Autowired
    private WithdrawalTestDao withdrawalTestDao;

    @Autowired
    private ReportService reportService;

    @Autowired
    private ReportGenerator reportGenerator;

    @Autowired
    private ReportingSrv.Iface reportHandler;

    private ReportRequest request;

    @BeforeEach
    public void setUp() {
        ReportTimeRange reportTimeRange = new ReportTimeRange(
                temporalToString(getFromTime()),
                temporalToString(getToTime())
        );
        request = new ReportRequest(partyId, contractId, reportTimeRange);
    }

    @Test
    public void exceptionArgTest() throws TException {
        assertThrows(
                InvalidRequest.class,
                () -> reportHandler.generateReport(request, "kek"));
    }

    @Test
    public void fistfulReporterTest() throws TException, IOException {
        when(partyManagementService.getContract(anyString(), anyString())).thenReturn(new Contract());
        when(fileStorageService.saveFile(any())).thenReturn(UUID.randomUUID().toString());

        withdrawalTestDao.saveWithdrawalsDependencies(5);

        long reportId = reportHandler.generateReport(request, "withdrawalRegistry");

        schedulerEmulation();

        Report report = reportHandler.getReport(partyId, contractId, reportId);

        assertEquals(ReportStatus.created, report.getStatus());
        assertEquals(1, report.getFileDataIds().size());
        verify(fileStorageService, times(1)).saveFile(any());
    }

    private void schedulerEmulation() {
        var pendingReports = reportService.getPendingReports();
        reportGenerator.generateReportFile(pendingReports.get(0));
    }
}
