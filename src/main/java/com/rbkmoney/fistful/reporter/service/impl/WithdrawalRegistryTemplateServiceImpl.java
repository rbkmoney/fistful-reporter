package com.rbkmoney.fistful.reporter.service.impl;

import com.rbkmoney.fistful.reporter.domain.tables.pojos.Report;
import com.rbkmoney.fistful.reporter.domain.tables.pojos.Withdrawal;
import com.rbkmoney.fistful.reporter.dto.ReportType;
import com.rbkmoney.fistful.reporter.service.TemplateService;
import com.rbkmoney.fistful.reporter.service.WithdrawalService;
import com.rbkmoney.fistful.reporter.util.TimeUtil;
import lombok.RequiredArgsConstructor;
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
public class WithdrawalRegistryTemplateServiceImpl implements TemplateService {

    private static final int CELLS_COUNT = 5;
    private final WithdrawalService withdrawalService;

    @Override
    public String getTemplateType() {
        return ReportType.WITHDRAWAL_REGISTRY.getType();
    }

    @Override
    public boolean accept(String reportType) {
        return reportType.equals(ReportType.WITHDRAWAL_REGISTRY.getType());
    }

    @Override
    public void processReportFileByTemplate(Report report, OutputStream outputStream) throws IOException {
        ZoneId reportZoneId = ZoneId.of(report.getTimezone());
        String fromTime = TimeUtil.toLocalizedDate(report.getFromTime().toInstant(ZoneOffset.UTC), reportZoneId);
        String toTime = TimeUtil.toLocalizedDate(report.getToTime().minusNanos(1).toInstant(ZoneOffset.UTC), reportZoneId);
        LongAdder inc = new LongAdder();

        // keep 100 rows in memory, exceeding rows will be flushed to disk
        SXSSFWorkbook wb = new SXSSFWorkbook(100);
        Sheet sh = getSheet(wb);
        Font font = getFont(wb);
        CellStyle greyStyle = getCellStyle(wb);

        // first row
        createFirstRow(fromTime, toTime, inc, sh, font);

        // second row
        createSecondRow(inc, sh, font, greyStyle);

        List<Withdrawal> withdrawals = withdrawalService.getSucceededWithdrawalsByReport(report);

        withdrawals.forEach(
                withdrawal -> {
                    Row row = sh.createRow(inc.intValue());
                    row.createCell(0).setCellValue(getTime(withdrawal.getEventCreatedAt(), reportZoneId));
                    row.createCell(1).setCellValue(withdrawal.getWithdrawalId());
                    row.createCell(2).setCellValue(withdrawal.getAmount());
                    row.createCell(3).setCellValue(withdrawal.getCurrencyCode());
                    row.createCell(4).setCellValue(withdrawal.getFee());
                    inc.increment();
                }
        );

        wb.write(outputStream);
        outputStream.close();
        wb.dispose();
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
        secondRow.getCell(2).setCellValue("Сумма");
        secondRow.getCell(3).setCellValue("Валюта");
        secondRow.getCell(4).setCellValue("Комиссия");
    }

    private String getTime(LocalDateTime localDateTime, ZoneId reportZoneId) {
        return TimeUtil.toLocalizedDateTime(localDateTime.toInstant(ZoneOffset.UTC), reportZoneId);
    }
}
