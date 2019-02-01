package org.credentialengine.cer.gremlin

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.util.*
import java.util.zip.GZIPOutputStream

open class GraphPayloadParser(
        val sourcePool: GraphSourcePool,
        val relationships: Relationships,
        val relationshipsOnly: Boolean = false,
        val envelope: Envelope,
        val json: JsonObject,
        val contexts: JsonContexts) : Closeable {
    private val logger = KotlinLogging.logger {}

    private val contextTypes: ContextTypes = contexts.getContextTypes(envelope.context)!!

    fun doParse() {
        val id = extractId(json)
        val type = extractType(json)
        logger.info { "Parsing inner @graph object $id." }
        parseDocument(json, id, type, envelope)
    }

    fun parseDocument(json: JsonObject, id: String, type: String, envelope: Envelope?) {
        relationships.registerType(id, type)

        if (relationshipsOnly) {
            for (entry in json.entrySet()) {
                val key = entry.key
                val value = entry.value
                val entryType = detect(key, value)

                when (entryType) {
                    EntryType.LITERAL -> {
                        // Do nothing
                    }
                    EntryType.ARRAY_OF_LITERALS -> {
                        // Do nothing
                    }
                    EntryType.OBJECT -> {
                        parseRelatedDocument(value.asJsonObject, type, id, key)
                    }
                    EntryType.REFERENCE -> {
                        addRelationship(id, key, sliceId(value.asString))
                    }
                    EntryType.ARRAY_OF_REFERENCES -> {
                        for (itemEntry in value.asJsonArray) {
                            addRelationship(id, key, sliceId(itemEntry.asString))
                        }
                    }
                    EntryType.ARRAY_OF_OBJECTS -> {
                        for (itemEntry in value.asJsonArray) {
                            parseRelatedDocument(itemEntry.asJsonObject, type, id, key)
                        }
                    }
                    EntryType.NONE -> {
                        // Do nothing
                    }
                }
            }

            return
        }

        var v = findOrCreateV(id, type).property(
                VertexProperty.Cardinality.single,
                Constants.PAYLOAD_PROPERTY, compress(json.toString()))

        val literals = mutableListOf<Pair<String, JsonPrimitive>>()

        for (entry in json.entrySet()) {
            val key = entry.key
            val value = entry.value
            val entryType = detect(key, value)
            when (entryType) {
                EntryType.LITERAL -> {
                    val primitive = getPrimitive(key, value.asJsonPrimitive)
                    v = v.property(
                            VertexProperty.Cardinality.single,
                            key,
                            primitive.original)
                    if (primitive.transformed) {
                        v = v.property(
                                VertexProperty.Cardinality.single,
                                key + "__" + primitive.suffix,
                                primitive.new)
                    }
                }
                EntryType.ARRAY_OF_LITERALS -> {
                    for (itemEntry in value.asJsonArray) {
                        val primitive = getPrimitive(key, itemEntry.asJsonPrimitive)
                        literals.add(Pair(key, primitive.original) as Pair<String, JsonPrimitive>)

                        if (primitive.transformed) {
                            literals.add(Pair(key + "__" + primitive.suffix, primitive.new) as Pair<String, JsonPrimitive>)
                        }
                    }
                }
                EntryType.OBJECT -> {
                    parseRelatedDocument(value.asJsonObject, type, id, key)
                }
                EntryType.REFERENCE -> {
                    addRelationship(id, key, sliceId(value.asString))
                }
                EntryType.ARRAY_OF_REFERENCES -> {
                    for (itemEntry in value.asJsonArray) {
                        addRelationship(id, key, sliceId(itemEntry.asString))
                    }
                }
                EntryType.ARRAY_OF_OBJECTS -> {
                    for (itemEntry in value.asJsonArray) {
                        parseRelatedDocument(itemEntry.asJsonObject, type, id, key)
                    }
                }
                EntryType.NONE -> {
                    // Do nothing
                }
            }
        }

        if (envelope != null) {
            v = v.property(VertexProperty.Cardinality.single, "__created_at", envelope.createdAt)
            v = v.property(VertexProperty.Cardinality.single, "__updated_at", envelope.updatedAt)

            if (envelope.ctid == id) {
                v = v.property(VertexProperty.Cardinality.single, "@context", envelope.context)
            }
        }

        logger.debug { "Storing $id." }
        v.next()

        for (chunk in literals.chunked(10)) {
            logger.debug { "Adding literal chunk for $id." }
            var vlist = findV(id)
            for (literal in chunk) {
                vlist = vlist.property(VertexProperty.Cardinality.list, literal.first, literal.second)
            }
            vlist.next()
        }
    }

    private fun detect(key: String, value: JsonElement): EntryType {
        val refKey = isReferenceKey(key)

        if (value.isJsonArray) {
            var arr = value.asJsonArray
            if (arr.size() == 0) return EntryType.NONE
            if (arr.all { it.isJsonPrimitive }) {
                if (refKey) return EntryType.ARRAY_OF_REFERENCES
                return EntryType.ARRAY_OF_LITERALS
            }
            if (arr.all { it.isJsonObject }) return EntryType.ARRAY_OF_OBJECTS
            logger.warn { "Found a mixed or nested array. This is not supported." }
            return EntryType.NONE
        }

        if (value.isJsonObject) {
            return EntryType.OBJECT
        }

        if (value.isJsonPrimitive) {
            if (refKey) {
                return EntryType.REFERENCE
            }
            return EntryType.LITERAL
        }

        return EntryType.NONE
    }

    private fun isReferenceKey(key: String): Boolean {
        return key != "ceterms:ctid" && contexts.isRefKey(envelope.context, key)
    }

    private var completed = false

    protected val source = sourcePool.borrowSource()

    protected fun findOrCreateV(id: String, type: String): GraphTraversal<Vertex, Vertex> {
        if (id.startsWith(Constants.GENERATED_PREFIX)) {
            return createV(id, type).property(VertexProperty.Cardinality.single, Constants.GENERATED_PROPERTY, true)
        }

        if (source.g.V().has(Constants.GRAPH_ID_PROPERTY, id).hasNext()) {
            logger.debug { "Found existing resource for $id." }
            return recreateV(id, type)
        }

        return createV(id, type)
    }

    protected fun findV(id: String): GraphTraversal<Vertex, Vertex> {
        return source.g.V().has(Constants.GRAPH_ID_PROPERTY, id)
    }

    private fun recreateV(id: String, type: String): GraphTraversal<Vertex, Vertex> {
        preserveEdges(id, type)
        source.g.V().has(Constants.GRAPH_ID_PROPERTY, id).drop().iterate()
        return createV(id, type)
    }

    private fun createV(id: String, type: String): GraphTraversal<Vertex, Vertex> {
        return source.g.addV(type).property(VertexProperty.Cardinality.single, Constants.GRAPH_ID_PROPERTY, id)
    }

    private fun preserveEdges(id: String, type: String) {
        val g = source.g
        val inEdges = g.V().has(Constants.GRAPH_ID_PROPERTY, id).inE().asAdmin()
        logger.debug { "Backing up ${inEdges.clone().count().next()} relationships." }

        for (e in inEdges) {
            val fromV = g.V(e.outVertex().id())
            val fromMap = fromV.valueMap<ArrayList<String>>(Constants.GRAPH_ID_PROPERTY).next()
            val fromId = fromMap[Constants.GRAPH_ID_PROPERTY]?.get(0)

            val toV = g.V(e.inVertex().id())
            val toMap = toV.valueMap<ArrayList<String>>(Constants.GRAPH_ID_PROPERTY).next()
            val toId = toMap[Constants.GRAPH_ID_PROPERTY]?.get(0)

            val relType = e.label()

            if (fromId == null || toId == null) throw Error("Vertices were found in an inconsistent state while setting up relationship.")
            relationships.addRelationship(Relationship(fromId, relType, toId, null, type))
        }
    }

    open fun getPrimitive(key: String, jsonPrimitive: JsonPrimitive): GraphPrimitive {
        if (jsonPrimitive.isBoolean) {
            return GraphPrimitive(false, jsonPrimitive.asBoolean, null, null)
        }
        if (jsonPrimitive.isNumber) {
            val n = jsonPrimitive.asNumber
            if (jsonPrimitive.toString().contains(".")) {
                return GraphPrimitive(false, n.toDouble(), null, null)
            }
            return GraphPrimitive(false, n.toLong(), null, null)
        }
        val str = jsonPrimitive.asString
        return contextTypes.convertToPrimitive(key, str)
    }

    protected fun extractType(json: JsonObject): String {
        if (json.has("@type")) {
            return json["@type"].asString
        }
        return Constants.GENERIC_LABEL
    }

    protected fun addRelationship(fromId: String, relType: String, toId: String, fromType: String? = null, toType: String? = null) {
        relationships.addRelationship(Relationship(fromId, relType, toId, fromType, toType))
    }

    fun parse() {
        doParse()
        completed = true
    }

    protected fun parseRelatedDocument(
            json: JsonObject,
            fromType: String,
            fromId: String,
            relType: String) {
        val otherId = extractId(json)
        val otherType = extractType(json)
        addRelationship(fromId, relType, otherId, fromType, otherType)
        return parseDocument(json, otherId, otherType, null)
    }

    protected fun compress(data: String): String {
        val bos = ByteArrayOutputStream(data.length)
        val gzip = GZIPOutputStream(bos)
        gzip.write(data.toByteArray())
        gzip.close()
        val compressed = bos.toByteArray()
        bos.close()
        val encodedBytes = String(Base64.getEncoder().encode(compressed))
        return "compressed:$encodedBytes"
    }

    override fun close() {
        sourcePool.returnSource(source, !completed)
    }

    companion object {
        @JvmStatic
        fun sliceId(value: String): String {
            return value.split("/").last().toLowerCase()
        }

        @JvmStatic
        fun extractId(json: JsonObject): String {
            if (json.has("@id")) {
                return sliceId(json["@id"].asString)
            }
            val uuid = UUID.randomUUID().toString()
            return "${Constants.GENERATED_PREFIX}$uuid"
        }
    }
}
