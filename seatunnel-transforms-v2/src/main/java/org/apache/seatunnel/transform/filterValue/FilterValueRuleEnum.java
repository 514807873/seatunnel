package org.apache.seatunnel.transform.filterValue;

import org.apache.seatunnel.shade.org.apache.commons.lang3.math.NumberUtils;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum FilterValueRuleEnum {
    ;
    private static final Map<String, Function2Parameters<Object, String, Boolean>> RULE_MAP =
            new HashMap<>();

    static {
        RULE_MAP.put("equal", (object, value) -> String.valueOf(object).equals(value));
        RULE_MAP.put("notEqual", (object, value) -> !String.valueOf(object).equals(value));
        RULE_MAP.put(
                "in",
                (object, value) ->
                        Arrays.asList(value.split(",")).contains(String.valueOf(object)));
        RULE_MAP.put(
                "between",
                (object, value) -> {
                    String[] split = value.split(",");
                    if (split.length != 2) throw new IllegalArgumentException("介于规则必须传2个参数");
                    BigDecimal start = NumberUtils.createBigDecimal(split[0]);
                    BigDecimal end = NumberUtils.createBigDecimal(split[1]);
                    BigDecimal number = NumberUtils.createBigDecimal(object.toString());
                    return number.compareTo(start) >= 0 && number.compareTo(end) <= 0;
                });
    }

    // 新的递归应用规则方法
    public static Boolean applyRules(
            RuleGroup ruleGroup, SeaTunnelRow row, Map<String, Integer> columnIndex) {
        Boolean result = null; // 初始化为null以处理空规则组
        String logicalOperator = ruleGroup.getLogicalOperator().toUpperCase();

        for (Object ruleObj : ruleGroup.getRules()) {
            Boolean currentResult;

            ObjectMapper objectMapper = new ObjectMapper();
            if (ruleObj instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) ruleObj;
                if (map.containsKey("logicalOperator") && map.containsKey("rules")) {
                    // 是个条件组  是个RuleGroup
                    RuleGroup subGroup = objectMapper.convertValue(map, RuleGroup.class);
                    currentResult = applyRules(subGroup, row, columnIndex);
                } else if (map.containsKey("name")
                        && map.containsKey("operator")
                        && map.containsKey("value")) {
                    // 是条件 Rule
                    Rule rule = objectMapper.convertValue(map, Rule.class);
                    currentResult = applySingleRule(rule, row, columnIndex);
                } else {
                    throw new IllegalArgumentException("Unknown rule structure: " + map);
                }
            } else {
                throw new IllegalArgumentException("Unknown rule type: " + ruleObj.getClass());
            }

            // 根据逻辑运算符组合结果
            if (result == null) {
                result = currentResult;
            } else {
                switch (logicalOperator) {
                    case "AND":
                        result = result && currentResult;
                        if (!result) return false;
                        break;
                    case "OR":
                        result = result || currentResult;
                        if (result) return true;
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Unknown logical operator: " + logicalOperator);
                }
            }
        }

        return result != null ? result : true; // 空规则组默认返回true
    }

    // 应用单个规则
    private static Boolean applySingleRule(
            Rule rule, SeaTunnelRow row, Map<String, Integer> columnIndex) {
        Function2Parameters<Object, String, Boolean> ruleFunction =
                RULE_MAP.get(rule.getOperator());
        if (ruleFunction == null) {
            throw new IllegalArgumentException("Unknown rule operator: " + rule.getOperator());
        }

        Integer index = columnIndex.get(rule.getName());
        if (index == null) {
            throw new IllegalArgumentException("Field not found in row: " + rule.getName());
        }

        Object fieldValue = row.getField(index);
        if (fieldValue == null) {
            return false;
        }

        return ruleFunction.apply(fieldValue, rule.getValue());
    }

    @Deprecated
    public static Boolean applyRules(
            List<Rule> rules, SeaTunnelRow row, Map<String, Integer> columnIndex) {
        RuleGroup tempGroup = new RuleGroup();
        tempGroup.setLogicalOperator("AND");
        tempGroup.setRules(new ArrayList<>(rules));
        return applyRules(tempGroup, row, columnIndex);
    }
}
