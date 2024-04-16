package io.airbyte.cdk.command

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.kjetland.jackson.jsonSchema.JsonSchemaConfig
import com.kjetland.jackson.jsonSchema.JsonSchemaDraft
import com.kjetland.jackson.jsonSchema.JsonSchemaGenerator
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SchemaValidatorsConfig
import com.networknt.schema.SpecVersion
import io.airbyte.commons.exceptions.ConfigErrorException
import io.airbyte.commons.jackson.MoreMappers
import io.airbyte.protocol.models.v0.AirbyteGlobalState
import io.airbyte.protocol.models.v0.AirbyteStateMessage
import io.airbyte.protocol.models.v0.AirbyteStream
import io.airbyte.protocol.models.v0.AirbyteStreamNameNamespacePair
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.ConfigurationProperties
import io.micronaut.context.annotation.Prototype

private val logger = KotlinLogging.logger {}

const val CONNECTOR_CONFIG_PREFIX: String = "airbyte.connector.config"
const val CONNECTOR_CATALOG_PREFIX: String = "airbyte.connector.catalog"
const val CONNECTOR_STATE_PREFIX: String = "airbyte.connector.state"

@Prototype
@ConfigurationProperties(CONNECTOR_CONFIG_PREFIX)
private class ConfigJsonObjectSupplierImpl<T : ConnectorConfigurationJsonObjectBase>(
    micronautPropertiesFallback: T
) : ConnectorConfigurationJsonObjectSupplier<T> {

    var json: String? = null

    @Suppress("UNCHECKED_CAST")
    override val valueClass: Class<T> = micronautPropertiesFallback::class.java as Class<T>

    override val jsonSchema: JsonNode by lazy {
        JsonUtils.generator.generateJsonSchema(valueClass)
    }

    val value: T by lazy {
        JsonUtils.parseOne(valueClass, json, micronautPropertiesFallback)
    }

    override fun get(): T = value
}

@ConfigurationProperties(CONNECTOR_CATALOG_PREFIX)
private class ConfiguredAirbyteCatalogSupplierImpl : ConfiguredAirbyteCatalogSupplier {

    var json: String = "{}"

    val value: ConfiguredAirbyteCatalog by lazy {
        JsonUtils.parseOne(ConfiguredAirbyteCatalog::class.java, json).also { cat ->
            for (configuredStream in cat.streams) {
                val stream: AirbyteStream = configuredStream.stream
                if (stream.name == null) {
                    throw ConfigErrorException("Configured catalog is missing stream name.")
                }
            }
        }
    }

    override fun get(): ConfiguredAirbyteCatalog = value
}

@ConfigurationProperties(CONNECTOR_STATE_PREFIX)
private class ConnectorInputStateSupplierImpl : ConnectorInputStateSupplier {

    var json: String = "[]"

    val value: InputState by lazy {
        val list: List<AirbyteStateMessage> =
            JsonUtils.parseList(AirbyteStateMessage::class.java, json)
        if (list.isEmpty()) {
            return@lazy EmptyInputState
        }
        val type: AirbyteStateMessage.AirbyteStateType = list.first().type
        val isGlobal: Boolean =
            when (type) {
                AirbyteStateMessage.AirbyteStateType.GLOBAL -> true
                AirbyteStateMessage.AirbyteStateType.STREAM -> false
                else -> throw ConfigErrorException("unsupported state type $type")
            }
        val filtered: List<AirbyteStateMessage> = list.filter { it.type == type }
        if (filtered.size < list.size) {
            val n = list.size - filtered.size
            logger.warn { "Discarded $n state message(s) not of type $type." }
        }
        if (isGlobal) {
            if (filtered.size > 1) {
                logger.warn { "Discarded all but last global state message." }
            }
            val global: AirbyteGlobalState = filtered.first().global
            return@lazy GlobalInputState(
                GlobalStateValue(global.sharedState),
                global.streamStates.associate {
                    AirbyteStreamNameNamespacePair(
                        it.streamDescriptor.name,
                        it.streamDescriptor.namespace
                    ) to StreamStateValue(it.streamState)
                }
            )
        }
        val lastOfEachStream: Map<AirbyteStreamNameNamespacePair, StreamStateValue> =
            filtered
                .map { it.stream }
                .groupingBy {
                    AirbyteStreamNameNamespacePair(
                        it.streamDescriptor.name,
                        it.streamDescriptor.namespace
                    )
                }
                .reduce { _, _, msg -> msg }
                .mapValues { StreamStateValue(it.value.streamState) }
        if (lastOfEachStream.size < filtered.size) {
            logger.warn { "Discarded all but last stream state for each stream descriptor." }
        }
        return@lazy StreamInputState(lastOfEachStream)
    }

    override fun get(): InputState = value
}


private data object JsonUtils {

    fun <T> parseOne(klazz: Class<T>, json: String?, micronautFriendlyFallback: T? = null): T {
        val tree: JsonNode = if (json != null) {
            try {
                mapper.readTree(json)
            } catch (e: Exception) {
                throw ConfigErrorException("malformed json value while parsing for $klazz", e)
            }
        } else {
            mapper.valueToTree(micronautFriendlyFallback ?: listOf<Any>())
        }
        return parseList(klazz, tree).firstOrNull()
            ?: throw ConfigErrorException("missing json value while parsing for $klazz")
    }

    fun <T> parseList(elementClass: Class<T>, json: String?): List<T> {
        val tree: JsonNode = try {
            mapper.readTree(json ?: "[]")
        } catch (e: Exception) {
            throw ConfigErrorException("malformed json value while parsing for $elementClass", e)
        }
        return parseList(elementClass, tree)
    }

    fun <T> parseList(elementClass: Class<T>, tree: JsonNode): List<T> {
        val jsonList: List<JsonNode> = if (tree.isArray) tree.toList() else listOf(tree)
        val schemaNode: JsonNode = generator.generateJsonSchema(elementClass)
        val jsonSchema: JsonSchema = jsonSchemaFactory.getSchema(schemaNode, jsonSchemaConfig)
        for (element in jsonList) {
            val validationFailures = jsonSchema.validate(element)
            if (validationFailures.isNotEmpty()) {
                throw ConfigErrorException(
                    "$elementClass json schema violation: ${validationFailures.first()}"
                )
            }
        }
        return jsonList.map {
            try {
                mapper.treeToValue(it, elementClass)
            } catch (e: Exception) {
                throw ConfigErrorException("failed to map valid json to $elementClass ", e)
            }
        }
    }

    val generatorConfig: JsonSchemaConfig =
        JsonSchemaConfig.vanillaJsonSchemaDraft4()
            .withJsonSchemaDraft(JsonSchemaDraft.DRAFT_07)
            .withFailOnUnknownProperties(false)

    val generator = JsonSchemaGenerator(MoreMappers.initMapper(), generatorConfig)

    val mapper: ObjectMapper = MoreMappers.initMapper().apply {
        setSerializationInclusion(JsonInclude.Include.NON_NULL)
        registerModule(KotlinModule.Builder().build())
    }

    val jsonSchemaConfig = SchemaValidatorsConfig()

    val jsonSchemaFactory: JsonSchemaFactory =
        JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7)
}
