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
import io.airbyte.protocol.models.v0.StreamDescriptor
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
        val deduped: List<AirbyteStateMessage> = list
            .groupBy { msg: AirbyteStateMessage ->
                if (msg.stream == null) {
                    msg.type.toString()
                } else {
                    val desc: StreamDescriptor = msg.stream.streamDescriptor
                    AirbyteStreamNameNamespacePair(desc.name, desc.namespace).toString()
                }
            }
            .mapNotNull { (groupKey, groupValues) ->
                if (groupValues.size > 1) {
                    logger.warn {
                        "Discarded duplicated ${groupValues.size - 1} state message(s) " +
                            "for '$groupKey'."
                    }
                }
                groupValues.last()
            }
        val nonGlobalStreams: Map<AirbyteStreamNameNamespacePair, StreamStateValue> = deduped
            .mapNotNull { it.stream }
            .associate {
                AirbyteStreamNameNamespacePair(
                    it.streamDescriptor.name,
                    it.streamDescriptor.namespace
                ) to JsonUtils.parseUnvalidated(it.streamState, StreamStateValue::class.java)
            }
        val globalState: AirbyteGlobalState? = deduped
            .find { it.type == AirbyteStateMessage.AirbyteStateType.GLOBAL }
            ?.global
        if (globalState == null) {
            return@lazy StreamInputState(nonGlobalStreams)
        }
        val globalStateValue: GlobalStateValue =
            JsonUtils.parseUnvalidated(globalState.sharedState, GlobalStateValue::class.java)
        val globalStreams: Map<AirbyteStreamNameNamespacePair, StreamStateValue> = globalState
            .streamStates
            .associate {
                AirbyteStreamNameNamespacePair(
                    it.streamDescriptor.name,
                    it.streamDescriptor.namespace
                ) to JsonUtils.parseUnvalidated(it.streamState, StreamStateValue::class.java)
            }
        return@lazy GlobalInputState(globalStateValue, globalStreams, nonGlobalStreams)
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
        return jsonList.map { parseUnvalidated(it, elementClass) }
    }

    fun <T> parseUnvalidated(jsonNode: JsonNode, klazz: Class<T>): T =
        try {
            mapper.treeToValue(jsonNode, klazz)
        } catch (e: Exception) {
            throw ConfigErrorException("failed to map valid json to $klazz ", e)
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
