package com.qh.myconnect.config;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.qh.myconnect.dialect.JdbcConnectorErrorCode;
import com.qh.myconnect.dialect.JdbcConnectorException;
import com.qh.myconnect.dialect.JdbcDialect;
import com.qh.myconnect.dialect.JdbcDialectTypeMapper;
import com.qh.myconnect.utils.ClickHouseSslHelper;
import com.qh.myconnect.utils.OracleSslHelper;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class Util {
    public SeaTunnelRowType initTableField(
            Connection conn, JdbcDialect jdbcDialect, JdbcSinkConfig jdbcSinkConfig) {
        JdbcDialectTypeMapper jdbcDialectTypeMapper = jdbcDialect.getJdbcDialectTypeMapper();
        ArrayList<SeaTunnelDataType<?>> seaTunnelDataTypes = new ArrayList<>();
        ArrayList<String> fieldNames = new ArrayList<>();
        try {
            ResultSetMetaData resultSetMetaData =
                    jdbcDialect.getResultSetMetaData(conn, jdbcSinkConfig);
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                fieldNames.add(resultSetMetaData.getColumnLabel(i));
                seaTunnelDataTypes.add(jdbcDialectTypeMapper.mapping(resultSetMetaData, i));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new SeaTunnelRowType(
                fieldNames.toArray(new String[0]),
                seaTunnelDataTypes.toArray(new SeaTunnelDataType<?>[0]));
    }

    public SeaTunnelRow toInternal(ResultSet rs, SeaTunnelRowType typeInfo) throws SQLException {
        Object[] fields = new Object[typeInfo.getTotalFields()];
        for (int fieldIndex = 0; fieldIndex < typeInfo.getTotalFields(); fieldIndex++) {
            SeaTunnelDataType<?> seaTunnelDataType = typeInfo.getFieldType(fieldIndex);
            int resultSetIndex = fieldIndex + 1;
            switch (seaTunnelDataType.getSqlType()) {
                case STRING:
                    fields[fieldIndex] = rs.getString(resultSetIndex);
                    break;
                case BOOLEAN:
                    fields[fieldIndex] = rs.getBoolean(resultSetIndex);
                    break;
                case TINYINT:
                    fields[fieldIndex] = rs.getByte(resultSetIndex);
                    break;
                case SMALLINT:
                    fields[fieldIndex] = rs.getShort(resultSetIndex);
                    break;
                case INT:
                    fields[fieldIndex] = rs.getInt(resultSetIndex);
                    break;
                case BIGINT:
                    fields[fieldIndex] = rs.getLong(resultSetIndex);
                    break;
                case FLOAT:
                    fields[fieldIndex] = rs.getFloat(resultSetIndex);
                    break;
                case DOUBLE:
                    fields[fieldIndex] = rs.getDouble(resultSetIndex);
                    break;
                case DECIMAL:
                    fields[fieldIndex] = rs.getBigDecimal(resultSetIndex);
                    break;
                case DATE:
                    java.sql.Date sqlDate = rs.getDate(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlDate).map(e -> e.toLocalDate()).orElse(null);
                    break;
                case TIME:
                    Time sqlTime = rs.getTime(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTime).map(e -> e.toLocalTime()).orElse(null);
                    break;
                case TIMESTAMP:
                    Timestamp sqlTimestamp = rs.getTimestamp(resultSetIndex);
                    fields[fieldIndex] =
                            Optional.ofNullable(sqlTimestamp)
                                    .map(e -> e.toLocalDateTime())
                                    .orElse(null);
                    break;
                case BYTES:
                    fields[fieldIndex] = rs.getBytes(resultSetIndex);
                    break;
                case NULL:
                    fields[fieldIndex] = null;
                    break;
                case MAP:
                case ARRAY:
                case ROW:
                default:
                    throw new JdbcConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unexpected value: " + seaTunnelDataType);
            }
        }
        return new SeaTunnelRow(fields);
    }

    public String Object2String(Object value) {
        if (null != value) {
            if (value instanceof java.util.Date) {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return formatter.format(((Date) value).toInstant());
            }
            else if (value instanceof Long) {
                return Long.toString((Long) value);
            }
            else if (value instanceof BigDecimal) {
                return ((BigDecimal) value).stripTrailingZeros().toPlainString();
            }
            else if (value instanceof LocalDate) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return ((LocalDate) value).format(df);
            }
            else if (value instanceof LocalDateTime) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                return df.format((LocalDateTime) value);
            }
            else if (value instanceof byte[]) {
                String newValue = Base64.getEncoder().encodeToString((byte[]) value);
                return "data:image/png;base64," + newValue;
            }
            else {
                return String.valueOf(value);
            }
        }
        return null;
    }

    public void insertLog(StatisticalLog statisticalLog) throws Exception {
        JSONObject param = new JSONObject();
        param.put("flinkJobId", statisticalLog.getFlinkJobId());
        param.put("dataSourceId", statisticalLog.getDataSourceId());
        if (null != statisticalLog.getDbSchema()) {
            param.put("dbSchema", statisticalLog.getDbSchema());
        }
        param.put("tableName", statisticalLog.getTableName());
        param.put("writeCount", statisticalLog.getWriteCount());
        param.put("modifyCount", statisticalLog.getModifyCount());
        param.put("deleteCount", statisticalLog.getDeleteCount());
        param.put("keepCount", statisticalLog.getKeepCount());
        param.put("insertCount", statisticalLog.getInsertCount());
        param.put("errorCount", statisticalLog.getErrorCount());
        param.put("qualityCount", statisticalLog.getQualityCount());
        param.put(
                "startTime",
                statisticalLog
                        .getStartTime()
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        param.put(
                "endTime",
                statisticalLog
                        .getEndTime()
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        String st_log_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/gatherJobLog";
        if(statisticalLog.getErrorMsg()!=null){
            param.put("errorMsg", statisticalLog.getErrorMsg());
        }
        this.sendPostRequest(st_log_url, param.toString());
    }

    public void insertErrorData(SeaTunnelJobsHistoryErrorRecord errorRecord) throws Exception {
        JSONObject param = new JSONObject();
        param.put("flinkJobId", errorRecord.getFlinkJobId());
        param.put("dataSourceId", errorRecord.getDataSourceId());
        if (null != errorRecord.getDbSchema()) {
            param.put("dbSchema", errorRecord.getDbSchema());
        }
        param.put("tableName", errorRecord.getTableName());
        param.put("errorData", errorRecord.getErrorData());
        param.put("errorMessage", errorRecord.getErrorMessage());
        String st_log_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/recordErrorData";
        this.sendPostRequest(st_log_url, param.toString());
    }

    public void setSubTaskStatus(SubTaskStatus subTaskStatus) throws Exception {
        JSONObject param = new JSONObject();
        param.put("flinkJobId", subTaskStatus.getFlinkJobId());
        param.put("dataSourceId", subTaskStatus.getDataSourceId());
        if (null != subTaskStatus.getDbSchema()) {
            param.put("dbSchema", subTaskStatus.getDbSchema());
        }
        param.put("tableName", subTaskStatus.getTableName());
        param.put("subtaskIndexId", subTaskStatus.getSubtaskIndexId());
        param.put("status", subTaskStatus.getStatus());
        String st_log_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/setSubTaskStatus";
        this.sendPostRequest(st_log_url, param.toString());
    }

    public List<SubTaskStatus> getSubTaskStatus(SubTaskStatus subTaskStatus) throws Exception {
        JSONObject param = new JSONObject();
        param.put("flinkJobId", subTaskStatus.getFlinkJobId());
        param.put("dataSourceId", subTaskStatus.getDataSourceId());
        if (null != subTaskStatus.getDbSchema()) {
            param.put("dbSchema", subTaskStatus.getDbSchema());
        }
        param.put("tableName", subTaskStatus.getTableName());
        String st_log_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/getSubTaskStatus";
        String response = this.sendPostRequest(st_log_url, param.toString());
        List<SubTaskStatus> list = new ArrayList<>();
        JSONObject jsonObject = JSONObject.parseObject(response);
        if (jsonObject.getInteger("code") == 10000) {
            JSONArray jsonArray = jsonObject.getJSONArray("result");
            for (Object value : jsonArray) {
                JSONObject tmp = (JSONObject) value;
                SubTaskStatus subTaskStatusReBean = JSON.toJavaObject(tmp, SubTaskStatus.class);
                list.add(subTaskStatusReBean);
            }
        }
        return list;
    }

    public java.sql.Driver loadDriver(String driverName) throws ClassNotFoundException {
        checkNotNull(driverName);
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            java.sql.Driver driver = drivers.nextElement();
            if (driver.getClass().getName().equals(driverName)) {
                return driver;
            }
        }
        Class<?> clazz =
                Class.forName(driverName, true, Thread.currentThread().getContextClassLoader());
        try {
            return (java.sql.Driver) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception ex) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.CREATE_DRIVER_FAILED,
                    "Fail to create driver of class " + driverName,
                    ex);
        }
    }

    public String sendPostRequest(String url, String data) throws Exception {
        URL apiUrl = new URL(url);
        HttpURLConnection con = (HttpURLConnection) apiUrl.openConnection();
        String result = null;
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Accept", "application/json");
        con.setDoOutput(true);
        OutputStream outputStream = con.getOutputStream();
        outputStream.write(data.getBytes());
        outputStream.flush();
        outputStream.close();

        // 处理响应
        int responseCode = con.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {

            InputStream is = con.getInputStream();
            // 缓冲流包装字符输入流,放入内存中,读取效率更快
            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            StringBuffer stringBuffer1 = new StringBuffer();
            String line = null;
            while ((line = br.readLine()) != null) {
                // 将每次读取的行进行保存
                stringBuffer1.append(line);
            }
            result = stringBuffer1.toString();
        }
        else {
            // 处理失败响应
        }
        con.disconnect();
        return result;
    }

    public Connection getConnection(JdbcSinkConfig jdbcSinkConfig) {
        try {
            Driver driver = this.loadDriver(jdbcSinkConfig.getDriver());
            Properties info = new Properties();
            if (jdbcSinkConfig.getUser() != null) {
                info.setProperty("user", jdbcSinkConfig.getUser());
            }
            if (jdbcSinkConfig.getPassWord() != null) {
                info.setProperty("password", jdbcSinkConfig.getPassWord());
            }
            String url =
                    OracleSslHelper.prepare(
                            jdbcSinkConfig.getDbType(),
                            jdbcSinkConfig.getUrl(),
                            info,
                            jdbcSinkConfig.getUseSsl(),
                            jdbcSinkConfig.getCaCert());
            url =
                    ClickHouseSslHelper.prepare(
                            jdbcSinkConfig.getDbType(),
                            url,
                            info,
                            jdbcSinkConfig.getUseSsl(),
                            jdbcSinkConfig.getCaCert(),
                            jdbcSinkConfig.getSslMode());
            Connection conn = driver.connect(url, info);
            if (conn == null) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                        "No suitable driver found for " + jdbcSinkConfig.getUrl());
            }
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Connection getPanguConnection() {
        try {
            Driver driver = this.loadDriver("com.mysql.cj.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("user", System.getenv("PANGU_MYSQL_ROOT_USER"));
            info.setProperty("password", System.getenv("PANGU_MYSQL_ROOT_PASSWORD"));
            Connection conn = driver.connect(System.getenv("PANGU_MYSQL_URL"), info);
            if (conn == null) {
                throw new JdbcConnectorException(
                        JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                        "业务mysql无法连接，请检查");
            }
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void truncateTable(TruncateTable bean) {
        String st_truncate_url = System.getenv("ST_SERVICE_URL") + "/SeaTunnelJob/truncateTable";
        JSONObject param = new JSONObject();
        param.put("flinkJobId", bean.getFlinkJobId());
        param.put("dataSourceId", bean.getDataSourceId());
        param.put("tableName", bean.getTableName());
        try {
            for (int i = 0; i < 3600; i++) {
                String value = this.sendPostRequest(st_truncate_url, param.toString());
                if (value.equalsIgnoreCase("true")) {
                    break;
                }
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
