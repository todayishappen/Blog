package com.minzheng.blog.utils;


import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Mysql2Postgresql {

    /**
     *
     * @param sqlFilePath：表示mysql的sql脚本路径
     * @param postgresqlModel：表示postgersql中的模式
     */


    public void generatePostgreSql(String sqlFilePath, String postgresqlModel) throws IOException, JSQLParserException {

        List<Statement> dropStatement = new ArrayList<>();
        List<Statement> createStatement = new ArrayList<>();
        List<Statement> insertStatement = new ArrayList<>();
        // MySQL DDL路径
        String dDLs = FileUtils.readFileToString(new File(sqlFilePath), "UTF-8");
        Statements statements = CCJSqlParserUtil.parseStatements(dDLs);

        for (Statement s: statements.getStatements()) {
            if (s instanceof Drop) {
                dropStatement.add(s);
            } else if (s instanceof CreateTable) {
                createStatement.add(s);
            } else if (s instanceof Insert) {
                insertStatement.add(s);
            }
        }
        System.out.println("======Drop Table BEGIN=====");
        dropStatement
                .stream()
                .map(statement -> (Drop) statement)
                .forEach(ct -> System.out.println(ct.toString().replaceAll("`", "") + ";"));
        System.out.println("======Drop Table END=====\n");
        System.out.println("======Insert Data BEGIN=====");
        insertStatement
                .stream()
                .map(statement -> (Insert) statement)
                .forEach(ct -> System.out.println(ct.toString()
                        .replaceAll(ct.getTable().getName(), postgresqlModel +"." + ct.getTable().getName())
                        .replaceAll("`", "") + ";"));
        System.out.println("======Insert Data END=====\n");
        System.out.println("======Create Table BEGIN=====");
        createStatement
                .stream()
                .map(statement -> (CreateTable) statement).forEach(ct -> {
            Table table = ct.getTable();
            List<ColumnDefinition> columnDefinitions = ct.getColumnDefinitions();
            List<String> comments = new ArrayList<>();
            List<ColumnDefinition> collect = columnDefinitions.stream()
                    .peek(columnDefinition -> {
                        List<String> columnSpecStrings = columnDefinition.getColumnSpecStrings();
                        int commentIndex = getCommentIndex(columnSpecStrings);
                        if (commentIndex != -1) {
                            int commentStringIndex = commentIndex + 1;
                            String commentString = columnSpecStrings.get(commentStringIndex);

                            String commentSql = genCommentSql(table.toString(), columnDefinition.getColumnName(), commentString);
                            comments.add(commentSql);
                            columnSpecStrings.remove(commentStringIndex);
                            columnSpecStrings.remove(commentIndex);
                        }
                        columnDefinition.setColumnSpecStrings(columnSpecStrings);
                    }).collect(Collectors.toList());
            ct.setColumnDefinitions(collect);
            String createSQL = ct.toString()
                    .replaceAll("bigint \\([0-9]+\\)", "bigint")
//                    .replaceAll("varchar \\(255\\)", "varchar\\(255\\)")
                    .replaceAll("AUTO_INCREMENT", "")
                    .replaceAll("USING BTREE", "")
                    .replaceAll("`", "")
                    .replaceAll(" ENGINE = InnoDB = 191 DEFAULT CHARSET = utf8 ROW_FORMAT = COMPACT ", "")
                    .replaceAll("BIGINT UNIQUE NOT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("BIGINT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("BIGINT NOT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("INT NOT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("INT NULL AUTO_INCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replaceAll("IF NOT EXISTS", "")
                    .replaceAll("TINYINT", "SMALLINT")
                    .replaceAll("DATETIME", "TIMESTAMP")
                    .replaceAll(", PRIMARY KEY \\(\"id\"\\)", "")
                    .replaceAll("DEFAULT NULL", "NULL")
                    .replaceAll(", KEY [a-z_A-Z0-9]+ \\(.*\\)", ")")
                    .replaceAll(" ENGINE .* COMPACT ", "")
                    .replaceAll("datetime", "timestamp")
                    .replaceAll("int \\([0-9]+\\)", "int")
                    .replaceAll(", UNIQUE KEY [a-z_A-Z0-9]+ \\(.*\\) ", "")
                    .replaceAll("ENGINE .* = utf8", "")
                    .replaceAll(" user ", " \"user\" ");

            // 如果存在表注释
            if (createSQL.contains("COMMENT")) {
                createSQL = createSQL.substring(0, createSQL.indexOf("COMMENT"));
            }
            System.out.println(createSQL + ";");

            comments.forEach(t -> System.out.println(t.replaceAll("`", "") + ";"));
        });
        System.out.println("======Create Table END=====\n");
    }

    /**
     * 获得注释的下标
     *
     * @param columnSpecStrings columnSpecStrings
     * @return 下标
     */
    private static int getCommentIndex(List<String> columnSpecStrings) {
        for (int i = 0; i < columnSpecStrings.size(); i++) {
            if ("COMMENT".equalsIgnoreCase(columnSpecStrings.get(i))) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 生成COMMENT语句
     *
     * @param table        表名
     * @param column       字段名
     * @param commentValue 描述文字
     * @return COMMENT语句
     */
    private static String genCommentSql(String table, String column, String commentValue) {
        return String.format("COMMENT ON COLUMN %s.%s IS %s", table, column, commentValue);
    }

    public static void main(String[] args) throws IOException, JSQLParserException {
        String mysqlDDLPath = "F:\\2024project\\blog_20240130\\Blog-master\\blog.sql";
        Mysql2Postgresql mysql2Postgresql = new Mysql2Postgresql();
        mysql2Postgresql.generatePostgreSql(mysqlDDLPath, "F:\\2024project\\blog_20240130\\Blog-master\\test20240202.sql");
    }
}