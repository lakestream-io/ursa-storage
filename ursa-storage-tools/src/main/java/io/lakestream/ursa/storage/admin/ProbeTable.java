/*
 * SPDX-FileCopyrightText: 2026 OpenLakestream contributors <https://openlakestream.org>
 * SPDX-License-Identifier: Apache-2.0
 */
package io.lakestream.ursa.storage.admin;

import com.google.gson.Gson;
import io.lakestream.ursa.lakehouse.LakehouseConfiguration;
import io.lakestream.ursa.lakehouse.iceberg.IcebergSinkConfig;
import io.lakestream.ursa.lakehouse.iceberg.IcebergTable;
import io.lakestream.ursa.lakehouse.iceberg.TableOptions;
import io.lakestream.ursa.lakehouse.iceberg.Utilities;
import io.lakestream.ursa.lakehouse.writer.ParquetFileStat;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Command to probe a table.
 */
@Command(name = "probe", description = "Probe a table")
public class ProbeTable implements Callable<Integer> {

    @CommandLine.ParentCommand
    private Table parent;

    @Option(
        names = {"-n", "--namespaces"},
        description = "Namespaces to probe, use comma to separate multiple namespaces",
        required = false,
        split = ","
    )
    private List<String> namespaces = List.of("public", "default");

    @Override
    public Integer call() throws Exception {
        try {
            String configFile = parent.getParent().getConfigFile();
            Properties properties = new Properties();
            try (FileInputStream fis = new FileInputStream(configFile)) {
                properties.load(fis);
            }
            LakehouseConfiguration configuration = new LakehouseConfiguration(properties);
            Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "data", Types.StringType.get())
            );
            TableOptions tableOptions = TableOptions.builder()
                .schema(schema)
                .build();
            System.out.println("Do probe under the namespace " + namespaces);
            TableIdentifier identifier = TableIdentifier.of(
                Namespace.of(namespaces.toArray(new String[0])),
                "probe-table");
            IcebergTable table = new IcebergTable(configuration, tableOptions, identifier);
            System.out.println("Creating table with the identifier: " + identifier);
            table.createIfAbsent();
            var taskWriter = Utilities.createTableWriter(table.getTable(), table.getTable().schema(), 1,
                new IcebergSinkConfig(configuration.getProperties()));

            System.out.println("Table location is " + table.getTable().location());

            System.out.println("Writing records to the table...");
            for (int i = 0; i < 10; i++) {
                Record record = GenericRecord.create(schema);
                record.setField("id", i);
                record.setField("data", "record-" + i);
                taskWriter.write(record);
            }
            var wr = taskWriter.complete();
            taskWriter.close();

            System.out.println("Write results info " + new Gson().toJson(wr));

            System.out.println("Records written successfully. Committing external file stats.");
            table.commitExternal(List.of(ParquetFileStat.fromWriteResults(List.of(wr), Map.of())));
            table.close();

            System.out.println("Table probing completed successfully.");
            return 0;
        } catch (Exception e) {
            System.err.println("Error probing table: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }
}
