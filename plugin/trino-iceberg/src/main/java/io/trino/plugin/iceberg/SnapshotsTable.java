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
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.iceberg.IcebergUtil.buildTableScan;
import static io.trino.plugin.iceberg.IcebergUtil.columnNameToPositionInSchema;
import static io.trino.plugin.iceberg.IcebergUtil.forEachRowInTableScan;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataTableType.SNAPSHOTS;

public class SnapshotsTable
        implements SystemTable
{
    private final ConnectorTableMetadata tableMetadata;
    private final Table icebergTable;
    private static final String COMMITTED_AT_COLUMN_NAME = "committed_at";
    private static final String SNAPSHOT_ID_COLUMN_NAME = "snapshot_id";
    private static final String PARENT_ID_COLUMN_NAME = "parent_id";
    private static final String OPERATION_COLUMN_NAME = "operation";
    private static final String MANIFEST_LIST_COLUMN_NAME = "manifest_list";
    private static final String SUMMARY_COLUMN_NAME = "summary";

    public SnapshotsTable(SchemaTableName tableName, TypeManager typeManager, Table icebergTable)
    {
        requireNonNull(typeManager, "typeManager is null");

        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        tableMetadata = new ConnectorTableMetadata(requireNonNull(tableName, "tableName is null"),
                ImmutableList.<ColumnMetadata>builder()
                        .add(new ColumnMetadata(COMMITTED_AT_COLUMN_NAME, TIMESTAMP_TZ_MILLIS))
                        .add(new ColumnMetadata(SNAPSHOT_ID_COLUMN_NAME, BIGINT))
                        .add(new ColumnMetadata(PARENT_ID_COLUMN_NAME, BIGINT))
                        .add(new ColumnMetadata(OPERATION_COLUMN_NAME, VARCHAR))
                        .add(new ColumnMetadata(MANIFEST_LIST_COLUMN_NAME, VARCHAR))
                        .add(new ColumnMetadata(SUMMARY_COLUMN_NAME, typeManager.getType(TypeSignature.mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()))))
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
        TableScan tableScan = buildTableScan(icebergTable, SNAPSHOTS);
        TimeZoneKey timeZoneKey = session.getTimeZoneKey();
        Map<String, Integer> columnNameToPosition = columnNameToPositionInSchema(tableScan.schema());

        forEachRowInTableScan(tableScan, structLike -> {
            pagesBuilder.beginRow();

            pagesBuilder.appendTimestampTzMillis(
                    structLike.get(columnNameToPosition.get(COMMITTED_AT_COLUMN_NAME), Long.class) / MICROSECONDS_PER_MILLISECOND,
                    timeZoneKey);
            pagesBuilder.appendBigint(structLike.get(columnNameToPosition.get(SNAPSHOT_ID_COLUMN_NAME), Long.class));

            Long parentId = structLike.get(columnNameToPosition.get(PARENT_ID_COLUMN_NAME), Long.class);
            pagesBuilder.appendBigint(parentId != null ? parentId.longValue() : null);

            pagesBuilder.appendVarchar(structLike.get(columnNameToPosition.get(OPERATION_COLUMN_NAME), String.class));
            pagesBuilder.appendVarchar(structLike.get(columnNameToPosition.get(MANIFEST_LIST_COLUMN_NAME), String.class));
            pagesBuilder.appendVarcharVarcharMap(structLike.get(columnNameToPosition.get(SUMMARY_COLUMN_NAME), Map.class));
            pagesBuilder.endRow();
        });

        return pagesBuilder.build();
    }
}
