package org.credentialengine.cer.gremlin

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.structure.VertexProperty

class GraphPayloadParser(
        traversal: GraphSourcePool,
        relationships: Relationships,
        val envelope: Envelope,
        val json: JsonObject,
        val contexts: JsonContexts) : PayloadParser(traversal, relationships) {
    private val logger = KotlinLogging.logger {}

    override fun doParse() {
        val id = extractId(json)
        val type = extractType(json)
        logger.info { "Parsing inner @graph object $id." }
        parseDocument(json, id, type, envelope)
    }

    override fun parseDocument(json: JsonObject, id: String, type: String, envelope: Envelope?) {
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
                    v = v.property(VertexProperty.Cardinality.single, key, getPrimitive(value.asJsonPrimitive))
                }
                EntryType.ARRAY_OF_LITERALS -> {
                    for (itemEntry in value.asJsonArray) {
                        literals.add(Pair(key, getPrimitive(itemEntry.asJsonPrimitive)) as Pair<String, JsonPrimitive>)
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
        return key != "ceterms:ctid" && contexts.isRefKey(envelope.processedResource["@context"].asString, key)
    }
}
