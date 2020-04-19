package com.yineng.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class MySqlUtil {
    private String query = "";
    private String username = ExecutionEnvUtil.PARAMETER_TOOL.get("mysql.username");
    private String password = ExecutionEnvUtil.PARAMETER_TOOL.get("mysql.password");
    private String driverName = "com.mysql.jdbc.Driver";
    private String dbUrl = ExecutionEnvUtil.PARAMETER_TOOL.get("mysql.r_warehouse.jdbc");
    private String tableName;
    private JDBCAppendTableSink sink;
    private StreamTableEnvironment tEnv;
    private TypeInformation<?>[] fieldTypes;
    private int[] sqlField;
    private String[] fieldNames;


    public static MySqlUtil builder() {
        MySqlUtil mySqlUtil = new MySqlUtil();
        return mySqlUtil;
    }
    public static MySqlUtil builder(StreamTableEnvironment tEnv) {
        MySqlUtil mySqlUtil = new MySqlUtil();
        mySqlUtil.tEnv = tEnv;
        return mySqlUtil;
    }
    public MySqlUtil setQuery(String query) {
        this.query = query;
        return this;
    }

    public MySqlUtil setEnv(StreamTableEnvironment tEnv) {
        this.tEnv = tEnv;
        return this;
    }
    public MySqlUtil setSqlField(int[] sqlField) {
        this.sqlField = sqlField;
        return this;
    }
    public MySqlUtil setFieldTypes(TypeInformation<?>[] fieldTypes) {
        this.fieldTypes = fieldTypes;
        return this;
    }
    public MySqlUtil setFieldNames( String[] fieldNames) {
        this.fieldNames = fieldNames;
        return this;
    }

    public MySqlUtil registerTable(String tableName) {
        this.tableName = tableName;
        this.sink = JDBCAppendTableSink.builder().setDrivername(driverName).setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setParameterTypes(fieldTypes)
                .setQuery(query)
                .setBatchSize(2)
                .build();

//        DataStream<Row> rowDataStream = this.tEnv.toAppendStream(this.table, Row.class);
//        this.sink.emitDataStream(rowDataStream);
//        TableSink<Row> configure1 = sink.configure(fieldNames, fieldTypes);
//        this.tEnv.registerTableSink(tableName, configure1);

//        table.insertInto("result_table");
        return this;
    }

    /**
     * stream 写入mysql
     * @param rowDataStream
     */
    public void streamSink(DataStream rowDataStream) {
//        DataStream<Row> rowDataStream = this.tEnv.toAppendStream(table, Row.class);

        this.sink.emitDataStream(rowDataStream);
    }

    /**
     * table 写入mysql
     * @param table
     */
    public void tableSink(Table table) {
        TableSink<Row> configure1 = sink.configure(fieldNames, fieldTypes);
        table.insertInto(this.tableName);
    }
    /*
    jdbc 写入mysql
     */
    public JDBCOutputFormat finish() {
        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setQuery(query)
                .setSqlTypes(sqlField)
                .setUsername(username)
                .setPassword(password)
                .setBatchInterval(1)
                .finish();
        return jdbcOutput;
    }

}
