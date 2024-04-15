/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.operation

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.ConfiguredAirbyteCatalogSupplier
import io.airbyte.cdk.command.ConnectorConfigurationSupplier
import io.airbyte.cdk.command.ConnectorInputStateSupplier
import io.airbyte.cdk.command.SourceConnectorConfiguration
import io.airbyte.cdk.consumers.CatalogValidationFailureHandler
import io.airbyte.cdk.consumers.OutputConsumer
import io.airbyte.cdk.jdbc.ColumnMetadata
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.jdbc.MetadataQuerier
import io.airbyte.cdk.jdbc.SelectFrom
import io.airbyte.cdk.jdbc.SourceOperations
import io.airbyte.cdk.jdbc.TableName
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.json.Jsons
import io.airbyte.protocol.models.v0.AirbyteMessage
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStateStats
import io.airbyte.protocol.models.v0.AirbyteStreamState
import io.airbyte.protocol.models.v0.CatalogHelpers
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.micronaut.context.annotation.Requires
import jakarta.inject.Singleton
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet


@Singleton
@Requires(property = CONNECTOR_OPERATION, value = "read")
@Requires(env = ["source"])
class ReadOperation(
    val configSupplier: ConnectorConfigurationSupplier<SourceConnectorConfiguration>,
    val catalogSupplier: ConfiguredAirbyteCatalogSupplier,
    val stateSupplier: ConnectorInputStateSupplier,
    val sourceOperations: SourceOperations,
    val metadataQuerier: MetadataQuerier,
    val jdbcConnectionFactory: JdbcConnectionFactory
    val outputConsumer: OutputConsumer,
    val validationHandler: CatalogValidationFailureHandler,
) : Operation, AutoCloseable {

    override val type = OperationType.READ

    override fun execute() {
        validateInputState()
        val selectFroms: List<SelectFrom> = collectSelectFroms()
        for (selectFrom in selectFroms) {
            val conn: Connection = jdbcConnectionFactory.get()
            try {
                conn.isReadOnly = true
                selectFrom.table.catalog?.let { conn.catalog = it }
                selectFrom.table.schema?.let { conn.schema = it }
                val q = sourceOperations.selectFrom(selectFrom)
                val stmt: PreparedStatement = conn.prepareStatement(q.sql)
                var rowCount = 0L
                var cursorValues: List<String> = selectFrom.cursorColumnValues
                try {
                    q.params.forEachIndexed { i, v -> stmt.setString(i+1, v) }
                    val rs: ResultSet = stmt.executeQuery()
                    while (rs.next()) {
                        cursorValues = sourceOperations.cursorColumnValues(selectFrom, rs)
                        val row = sourceOperations.dataColumnValues(selectFrom, rs)
                        recordConsumer(selectFrom, row)
                        rowCount++
                    }
                    val streamState: JsonNode =
                        Jsons.jsonNode(selectFrom.cursorColumnNames.zip(cursorValues).toMap())
                    outputConsumer.accept(AirbyteMessage()
                        .withType(AirbyteMessage.Type.STATE)
                        .withState(AirbyteStateMessage()
                            .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
                            .withStream(AirbyteStreamState()
                                .withStreamDescriptor(StreamDescriptor()
                                    .withName(selectFrom.configuredStream.stream.name)
                                    .withNamespace(selectFrom.configuredStream.stream.namespace))
                                .withStreamState(streamState))
                            .withSourceStats(AirbyteStateStats()
                                .withRecordCount(rowCount.toDouble()))))
                } finally {
                    stmt.close()
                }
            } finally {
                conn.close()
            }
        }
    }

    private fun recordConsumer(selectFrom: SelectFrom, row: List<Any?>) {

    }

    private fun validateInputState() {
        val config: SourceConnectorConfiguration = configSupplier.get()
        val inputState: List<AirbyteStateMessage> = stateSupplier.get()
        if (inputState.isEmpty()) {
            return
        }
        val actualType: AirbyteStateMessage.AirbyteStateType = inputState.first().type
        if (config.expectedStateType != actualType) {
            throw ConfigErrorException(
                "Provided state of type $actualType is incompatible with connector " +
                    "configuration requirements for state type ${config.expectedStateType} " +
                    "for READ operation."
            )
        }
    }

    private fun collectSelectFroms(): List<SelectFrom> {
        val configuredCatalog: ConfiguredAirbyteCatalog = catalogSupplier.get()
        val tableNames: List<TableName> = metadataQuerier.tableNames()
        return configuredCatalog.streams.mapNotNull { toSelectFrom(it, tableNames) }
    }

    private fun toSelectFrom(configuredStream: ConfiguredAirbyteStream, tableNames: List<TableName>): SelectFrom? {
        val stream = configuredStream.stream
        val name: String = stream.name!!
        val namespace: String? = stream.namespace
        val matchingTables: List<TableName> = tableNames
            .filter { it.name == name }
            .filter { it.catalog == namespace || it.schema == namespace || namespace == null }
        val table: TableName = when (matchingTables.size) {
            0 -> {
                validationHandler.tableNotFound(name, namespace)
                return null
            }
            1 -> matchingTables.first()
            else -> {
                validationHandler.multipleTablesFound(name, namespace, matchingTables)
                return null
            }
        }
        val expectedColumnNames: Set<String> = CatalogHelpers.getTopLevelFieldNames(configuredStream)
        val dataColumns: List<ColumnMetadata> = metadataQuerier.columnMetadata(table)
            .filter { expectedColumnNames.contains(it.name) }
        for (columnName in expectedColumnNames.toList().sorted()) {
            if (columnName.startsWith("_ab_")) {
                // Ignore airbyte metadata columns.
                // These aren't actually present in the table.
                continue
            }
            val columnMetadata: ColumnMetadata? = dataColumns.find { it.name == columnName }
            if (columnMetadata == null) {
                validationHandler.columnNotFound(name, namespace, columnName)
                continue
            }
            val expectedSchema: JsonNode =
                configuredStream.stream.jsonSchema["properties"][columnName]
            val actualSchema: JsonNode =
                Jsons.jsonNode(sourceOperations.toAirbyteType(columnMetadata).jsonSchemaTypeMap)
            if (expectedSchema != actualSchema) {
                validationHandler.columnTypeMismatch(
                    name, namespace, columnName, expectedSchema, actualSchema)
                continue
            }
        }
        return SelectFrom(configuredStream, table, dataColumns, listOf(), listOf(), null)
    }



    override fun close() {
        metadataQuerier.close()
    }
}
