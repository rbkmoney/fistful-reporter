package com.rbkmoney.fistful.reporter.utils;

import com.rbkmoney.fistful.reporter.util.FormatUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FormatUtilTest {

    @Test
    public void testFormatCurrency() {
        assertEquals(1.0, FormatUtil.formatCurrency(100, "RUB"), 0.0);
        assertEquals(1.0, FormatUtil.formatCurrency(100, "UAH"), 0.0);
        assertEquals(100.0, FormatUtil.formatCurrency(100, "JPY"), 0.0);
        assertEquals(0.01, FormatUtil.formatCurrency(100, "CLF"), 0.0);
    }

}
