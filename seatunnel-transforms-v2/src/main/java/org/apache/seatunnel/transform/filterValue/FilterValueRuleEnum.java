package org.apache.seatunnel.transform.filterValue;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum FilterValueRuleEnum {
    ;
    private static final Map<String, Function2Parameters<Object, String, Boolean>> RULE_MAP = new HashMap<>();
    private static final Function2Parameters<Object, String, Boolean> equal = (object, value) -> String.valueOf(object).equals(value);
    private static final Function2Parameters<Object, String, Boolean> notEqual =
            (object, value) -> !String.valueOf(object).equals(value);
    private static final Function2Parameters<Object, String, Boolean> in = (object, value) -> {
        List<String> list = Arrays.asList(value.split(","));
        return list.contains(String.valueOf(object));
    };
    private static final Function2Parameters<Object, String, Boolean> between = (object, value) -> {
        String[] split = value.split(",");
        if (split.length != 2) {
            throw new IllegalArgumentException("介于规则必须传2个参数");
        }
        BigDecimal start = NumberUtils.createBigDecimal(split[0]);
        BigDecimal end = NumberUtils.createBigDecimal(split[1]);
        BigDecimal number = NumberUtils.createBigDecimal(object.toString());
        return number.compareTo(start) >= 0 && number.compareTo(end) <= 0;
    };

    static {
        RULE_MAP.put("equal", equal);
        RULE_MAP.put("notEqual", notEqual);
        RULE_MAP.put("in", in);
        RULE_MAP.put("between", between);
    }

    public static Boolean applyRules(List<Rule> rules, SeaTunnelRow row, Map<String, Integer> columnIndex) {
        boolean result = true;
        for (Rule rule : rules) {
            Function2Parameters<Object, String, Boolean> ruleFunction = RULE_MAP.get(rule.getOperator());
            if (ruleFunction != null) {
                Object field = row.getField(columnIndex.get(rule.getName()));
                if (field == null) {
                    return false;
                }
                Boolean tmpResult = ruleFunction.apply(field, rule.getValue());
                if (!tmpResult) {
                    result = false;
                }
            }
            else {
                throw new IllegalArgumentException("Unknown rule: " + rule.getOperator());
            }
            if (!result) {
                break;
            }
        }
        return result;
    }
}
