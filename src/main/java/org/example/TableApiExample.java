package org.example;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

public class TableApiExample {

    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.createTemporaryTable("SourceTable",
                TableDescriptor.forConnector("datagen")
                        .schema(Schema.newBuilder()
                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .build())
                        .option(DataGenConnectorOptions.ROWS_PER_SECOND, 1L)
                        .build());

        tableEnv.createTemporaryTable("SinkTable",
                TableDescriptor.forConnector("print")
                        .schema(Schema.newBuilder()
//                                .column("f0", DataTypes.STRING())
                                .column("f1", DataTypes.BIGINT())
                                .build())
                        .build());

        Table table1 = tableEnv.from("SourceTable");

        Table table2 = tableEnv.sqlQuery("SELECT f1 FROM SourceTable");

        table2.insertInto("SinkTable").execute();

        // flink run -c org.example.TableApiExample target/flink-example-1.0.0.jar
    }
}
