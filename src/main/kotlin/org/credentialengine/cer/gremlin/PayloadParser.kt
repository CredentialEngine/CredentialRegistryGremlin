package org.credentialengine.cer.gremlin

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import java.io.Closeable
import java.util.*
import java.util.zip.GZIPOutputStream
import java.io.ByteArrayOutputStream
import kotlin.collections.ArrayList

abstract class PayloadParser(protected val sourcePool: GraphSourcePool,
                             protected val relationships: Relationships,
                             protected val jsonParent: JsonObject) : Closeable {
    private var completed = false

    private val logger = KotlinLogging.logger {}

    protected val source = sourcePool.borrowSource()

    protected fun findOrCreateV(id: String, type: String): GraphTraversal<Vertex, Vertex> {
        if (id.startsWith(Constants.GENERATED_PREFIX)) {
            return createV(id, type).property(VertexProperty.Cardinality.single, Constants.GENERATED_PROPERTY, true)
        }

        if (source.g.V().has(Constants.GRAPH_ID_PROPERTY, id).hasNext()) {
            logger.debug{"Found existing resource for $id."}
            return recreateV(id, type)
        }

        return createV(id, type)
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
        logger.debug{"Backing up ${inEdges.clone().count().next()} relationships."}

        for (e in inEdges)
        {
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

    protected fun getPrimitive(jsonPrimitive: JsonPrimitive): Any? {
        if (jsonPrimitive.isBoolean) {
            return jsonPrimitive.asBoolean
        }
        if (jsonPrimitive.isNumber) {
            val n = jsonPrimitive.asNumber
            if (jsonPrimitive.toString().contains(".")) {
                return n.toDouble()
            }
            return n.toLong()
        }
        return jsonPrimitive.asString
    }

    protected fun getArrayOfPrimitives(jsonArray: JsonArray): Array<Any?> {
        return jsonArray.map { it -> getPrimitive(it.asJsonPrimitive) }.toTypedArray()
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

    abstract fun doParse()

    protected abstract fun parseDocument(
            json: JsonObject,
            id: String,
            type: String)

    protected fun parseRelatedDocument(
            json: JsonObject,
            fromType: String,
            fromId: String,
            relType: String) {
        val otherId = extractId(json)
        val otherType = extractType(json)
        addRelationship(fromId, relType, otherId, fromType, otherType)
        return parseDocument(json, otherId, otherType)
    }

    protected fun compress(data: String): String {
        val bos = ByteArrayOutputStream(data.length)
        val gzip = GZIPOutputStream(bos)
        gzip.write(data.toByteArray())
        gzip.close()
        val compressed = bos.toByteArray()
        bos.close()
        val encodedBytes = Base64.getEncoder().encode(compressed)
        return String(encodedBytes)
    }

    override fun close() {
        sourcePool.returnSource(source, !completed)
    }

    companion object {
        @JvmStatic
        protected fun sliceId(value: String): String {
            return value.split("/").last()
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
