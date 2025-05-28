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
        if (rules == null || rules.isEmpty()) {
            return true;
        }

        // 对规则组进行整体评估
        return evaluateRuleGroup(rules, row, columnIndex, "AND"); // 最外层默认AND
    }

    private static Boolean evaluateRuleGroup(List<Rule> ruleGroup, SeaTunnelRow row,
                                             Map<String, Integer> columnIndex, String defaultLogicalOp) {
        if (ruleGroup == null || ruleGroup.isEmpty()) {
            return true;
        }

        Boolean groupResult = null;
        for (Rule rule : ruleGroup) {
            // 确定当前规则的实际逻辑运算符
            String currentLogicalOp = rule.getLogicalOperator() != null ? rule.getLogicalOperator() : defaultLogicalOp;

            // 评估当前规则
            Boolean currentResult = evaluateSingleRule(rule, row, columnIndex);
            if (currentResult == null) {
                continue; // 忽略无效规则
            }

            // 应用逻辑运算
            groupResult = applyLogicalOperation(groupResult, currentResult, currentLogicalOp);

            // 优化：提前终止条件
            if (shouldEarlyTerminate(groupResult, currentLogicalOp)) {
                break;
            }
        }

        return groupResult != null ? groupResult : true;
    }

    private static Boolean evaluateSingleRule(Rule rule, SeaTunnelRow row,
                                              Map<String, Integer> columnIndex) {
        // 处理嵌套规则
        if (rule.getRules() != null && !rule.getRules().isEmpty()) {
            // 嵌套规则组继承父规则的逻辑运算符
            return evaluateRuleGroup(rule.getRules(), row, columnIndex,
                    rule.getLogicalOperator() != null ?
                            rule.getLogicalOperator() : "AND");
        }

        // 处理普通规则
        if (rule.getOperator() != null && rule.getName() != null) {
            Function2Parameters<Object, String, Boolean> ruleFunction = RULE_MAP.get(rule.getOperator());
            if (ruleFunction != null) {
                Object field = row.getField(columnIndex.get(rule.getName()));
                if (field == null) {
                    return false;
                }
                return ruleFunction.apply(field, rule.getValue());
            }
            throw new IllegalArgumentException("未知的操作符: " + rule.getOperator());
        }

        return null; // 无效规则
    }

    private static Boolean applyLogicalOperation(Boolean accumulated, Boolean current, String logicalOp) {
        if (accumulated == null) {
            return current;
        }

        switch (logicalOp.toUpperCase()) {
            case "AND": return accumulated && current;
            case "OR": return accumulated || current;
            case "NOT": return !current;
            default: throw new IllegalArgumentException("不支持的逻辑运算符: " + logicalOp);
        }
    }

    private static boolean shouldEarlyTerminate(Boolean currentResult, String logicalOp) {
        if (currentResult == null) return false;

        // AND遇到false可以提前终止
        if ("AND".equalsIgnoreCase(logicalOp) && !currentResult) {
            return true;
        }
        // OR遇到true可以提前终止
        if ("OR".equalsIgnoreCase(logicalOp) && currentResult) {
            return true;
        }
        return false;
    }
}
