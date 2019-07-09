package com.rbkmoney.fistful.reporter.util;

import java.math.BigDecimal;
import java.util.Currency;

public class FormatUtil {

    public static double formatCurrency(long value, String currencyCode) {
        return BigDecimal.valueOf(value).movePointLeft(Currency.getInstance(currencyCode).getDefaultFractionDigits()).doubleValue();
    }

}
