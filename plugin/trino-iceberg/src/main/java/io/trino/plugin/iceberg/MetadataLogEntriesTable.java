/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TimeZoneKey;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergUtil.buildTableScan;
import static io.trino.plugin.iceberg.IcebergUtil.columnNameToPositionInSchema;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.METADATA_LOG_ENTRIES;

public class MetadataLogEntriesTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    private static final String FILE_COLUMN_NAME = "file";
    private static final String LATEST_SNAPSHOT_ID_COLUMN_NAME = "latest_snapshot_id";
    private static final String LATEST_SCHEMA_ID_COLUMN_NAME = "latest_schema_id";
    private static final String LATEST_SEQUENCE_NUMBER_COLUMN_NAME = "latest_sequence_number";

    public MetadataLogEntriesTable(SchemaTableName tableName, Table icebergTable)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");

        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata(TIMESTAMP_COLUMN_NAME, TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata(FILE_COLUMN_NAME, VARCHAR))
                        .add(new ColumnMetadata(LATEST_SNAPSHOT_ID_COLUMN_NAME, BIGINT))
                        .add(new ColumnMetadata(LATEST_SCHEMA_ID_COLUMN_NAME, INTEGER))
                        .add(new ColumnMetadata(LATEST_SEQUENCE_NUMBER_COLUMN_NAME, BIGINT))
                        .build());
    }

    @Override
    public Distribution getDistribution()
    {
        return Distribution.SINGLE_COORDINATOR;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata()
    {
        return tableMetadata;
    }

    @Override
    public ConnectorPageSource pageSource(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
    {
        return new FixedPageSource(buildPages(tableMetadata, session, icebergTable));
    }

    private static List<Page> buildPages(ConnectorTableMetadata tableMetadata, ConnectorSession session, Table icebergTable)
    {
        PageListBuilder pagesBuilder = PageListBuilder.forTable(tableMetadata);

        TableScan tableScan = buildTableScan(icebergTable, METADATA_LOG_ENTRIES);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();

        Map<String, Integer> columnNameToPosition = columnNameToPositionInSchema(tableScan.schema());

        try (CloseableIterable<FileScanTask> fileScanTasks = tableScan.planFiles()) {
            fileScanTasks.forEach(fileScanTask -> addRows((DataTask) fileScanTask, pagesBuilder, timeZoneKey, columnNameToPosition));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return pagesBuilder.build();
    }

    private static void addRows(DataTask dataTask, PageListBuilder pagesBuilder, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        try (CloseableIterable<StructLike> dataRows = dataTask.rows()) {
            dataRows.forEach(dataTaskRow -> addRow(pagesBuilder, dataTaskRow, timeZoneKey, columnNameToPositionInSchema));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void addRow(PageListBuilder pagesBuilder, StructLike structLike, TimeZoneKey timeZoneKey, Map<String, Integer> columnNameToPositionInSchema)
    {
        pagesBuilder.beginRow();

        pagesBuilder.appendTimestampTzMillis(
                structLike.get(columnNameToPositionInSchema.get(TIMESTAMP_COLUMN_NAME), Long.class) / MICROSECONDS_PER_MILLISECOND,
                timeZoneKey);
        pagesBuilder.appendVarchar(structLike.get(columnNameToPositionInSchema.get(FILE_COLUMN_NAME), String.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get(LATEST_SNAPSHOT_ID_COLUMN_NAME), Long.class));
        pagesBuilder.appendInteger(structLike.get(columnNameToPositionInSchema.get(LATEST_SCHEMA_ID_COLUMN_NAME), Integer.class));
        pagesBuilder.appendBigint(structLike.get(columnNameToPositionInSchema.get(LATEST_SEQUENCE_NUMBER_COLUMN_NAME), Long.class));
        pagesBuilder.endRow();
    }
}
