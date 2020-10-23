package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.dao.DaoException;
import com.rbkmoney.fistful.reporter.dao.WithdrawalDao;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.dto.ReportType;
import com.rbkmoney.fistful.reporter.exception.StorageException;
import com.rbkmoney.fistful.reporter.service.TemplateService;
import com.rbkmoney.fistful.reporter.util.FormatUtils;
import com.rbkmoney.fistful.reporter.util.TimeUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellUtil;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

@Component
@RequiredArgsConstructor
@Slf4j
public class WithdrawalRegistryTemplateServiceImpl implements TemplateService {

    private static final int CELLS_COUNT = 7;
    private static final int LIMIT = 1000;
    private final WithdrawalDao withdrawalDao;

    @Override
    public String getTemplateType() {
        return ReportType.WITHDRAWAL_REGISTRY.getType();
    }

    @Override
    public String getReportPrefixName() {
        return "Withdrawals_registry";
    }

    @Override
    public boolean accept(String reportType) {
        return reportType.equals(ReportType.WITHDRAWAL_REGISTRY.getType());
    }

    @Override
    public void processReportFileByTemplate(Report report, OutputStream outputStream) throws IOException {
        ZoneId reportZoneId = ZoneId.of(report.getTimezone());
        String fromTime = TimeUtils.toLocalizedDate(report.getFromTime().toInstant(ZoneOffset.UTC), reportZoneId);
        String toTime = TimeUtils.toLocalizedDate(report.getToTime().minusNanos(1).toInstant(ZoneOffset.UTC), reportZoneId);
        LongAdder inc = new LongAdder();

        // keep 100 rows in memory, exceeding rows will be flushed to disk
        try (SXSSFWorkbook wb = new SXSSFWorkbook(100)) {
            Sheet sh = getSheet(wb);
            Font font = getFont(wb);
            CellStyle greyStyle = getCellStyle(wb);

            // first row
            createFirstRow(fromTime, toTime, inc, sh, font);

            // second row
            createSecondRow(inc, sh, font, greyStyle);

            writeLimitRows(report, reportZoneId, inc, sh, 0);

            wb.write(outputStream);
            outputStream.close();
            wb.dispose();
        }
    }

    private void writeLimitRows(Report report, ZoneId reportZoneId, LongAdder inc, Sheet sh, long fromId) {
        List<Withdrawal> withdrawals = getSucceededWithdrawalsByReport(report, fromId);

        withdrawals.forEach(
                withdrawal -> {
                    Row row = sh.createRow(inc.intValue());
                    row.createCell(0).setCellValue(getTime(withdrawal.getEventCreatedAt(), reportZoneId));
                    row.createCell(1).setCellValue(withdrawal.getWithdrawalId());
                    row.createCell(2).setCellValue(withdrawal.getWalletId());
                    row.createCell(3).setCellValue(FormatUtils.formatCurrency(withdrawal.getAmount(), withdrawal.getCurrencyCode()));
                    row.createCell(4).setCellValue(withdrawal.getCurrencyCode());
                    row.createCell(5).setCellValue(FormatUtils.formatCurrency(withdrawal.getFee(), withdrawal.getCurrencyCode()));
                    row.createCell(6).setCellValue(withdrawal.getExternalId());
                    inc.increment();
                }
        );

        initNextWriting(report, reportZoneId, inc, sh, withdrawals);
    }

    private void initNextWriting(Report report, ZoneId reportZoneId, LongAdder inc, Sheet sh, List<Withdrawal> withdrawals) {
        int size = withdrawals.size();
        if (size == LIMIT) {
            Long fromId = withdrawals.get(size - 1).getId();

            withdrawals.clear();

            writeLimitRows(report, reportZoneId, inc, sh, fromId);
        }
    }

    private List<Withdrawal> getSucceededWithdrawalsByReport(Report report, long fromId) {
        try {
            log.info("Trying to get succeeded withdrawals by report, " +
                            "reportId={}, partyId={}, contractId={}, fromId={}",
                    report.getId(), report.getPartyId(), report.getContractId(), fromId);

            List<Withdrawal> withdrawals = withdrawalDao.getSucceededWithdrawalsByReport(report, fromId, LIMIT);

            log.info("{} succeeded withdrawals by report have been found, " +
                            "reportId={}, partyId={}, contractId={}, fromId={}",
                    withdrawals.size(), report.getId(), report.getPartyId(), report.getContractId(), fromId);

            return withdrawals;
        } catch (DaoException ex) {
            throw new StorageException("Failed to get succeeded withdrawals", ex);
        }
    }

    private Sheet getSheet(SXSSFWorkbook wb) {
        Sheet sh = wb.createSheet();
        sh.setDefaultColumnWidth(20);
        return sh;
    }

    private Font getFont(SXSSFWorkbook wb) {
        Font font = wb.createFont();
        font.setBold(true);
        return font;
    }

    private CellStyle getCellStyle(SXSSFWorkbook wb) {
        CellStyle greyStyle = wb.createCellStyle();
        greyStyle.setFillBackgroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        greyStyle.setFillPattern(FillPatternType.LESS_DOTS);
        return greyStyle;
    }

    private void createFirstRow(String fromTime, String toTime, LongAdder inc, Sheet sh, Font font) {
        Row firstRow = sh.createRow(inc.intValue());
        configUIFirstRow(sh, firstRow);

        Cell firstRowFirstCell = firstRow.getCell(0);
        configUIFirstRowFirstCell(font, firstRowFirstCell);
        initDataFirstRowFirstCell(fromTime, toTime, firstRowFirstCell);
        inc.increment();
    }

    private void configUIFirstRow(Sheet sh, Row firstRow) {
        for (int i = 0; i < CELLS_COUNT; i++) {
            firstRow.createCell(i);
        }
        sh.addMergedRegion(new CellRangeAddress(0, 0, 0, CELLS_COUNT - 1));
    }

    private void configUIFirstRowFirstCell(Font font, Cell firstRowFirstCell) {
        CellUtil.setAlignment(firstRowFirstCell, HorizontalAlignment.CENTER);
        CellUtil.setFont(firstRowFirstCell, font);
    }

    private void initDataFirstRowFirstCell(String fromTime, String toTime, Cell firstRowFirstCell) {
        firstRowFirstCell.setCellValue(String.format("Выводы за период с %s по %s", fromTime, toTime));
    }

    private void createSecondRow(LongAdder inc, Sheet sh, Font font, CellStyle greyStyle) {
        Row secondRow = sh.createRow(inc.intValue());
        configUISecondRowAllCells(font, greyStyle, secondRow);
        initDataSecondRowAllCell(secondRow);
        inc.increment();
    }

    private void configUISecondRowAllCells(Font font, CellStyle greyStyle, Row secondRow) {
        for (int i = 0; i < CELLS_COUNT; ++i) {
            Cell cell = secondRow.createCell(i);
            configUISecondRow(font, greyStyle, cell);
        }
    }

    private void configUISecondRow(Font font, CellStyle greyStyle, Cell cell) {
        CellUtil.setAlignment(cell, HorizontalAlignment.CENTER);
        cell.setCellStyle(greyStyle);
        CellUtil.setFont(cell, font);
    }

    private void initDataSecondRowAllCell(Row secondRow) {
        secondRow.getCell(0).setCellValue("Дата");
        secondRow.getCell(1).setCellValue("Id вывода");
        secondRow.getCell(2).setCellValue("Id кошелька");
        secondRow.getCell(3).setCellValue("Сумма");
        secondRow.getCell(4).setCellValue("Валюта");
        secondRow.getCell(5).setCellValue("Комиссия");
        secondRow.getCell(6).setCellValue("Уникальный идентификатор");
    }

    private String getTime(LocalDateTime localDateTime, ZoneId reportZoneId) {
        return TimeUtils.toLocalizedDateTime(localDateTime.toInstant(ZoneOffset.UTC), reportZoneId);
    }
}
