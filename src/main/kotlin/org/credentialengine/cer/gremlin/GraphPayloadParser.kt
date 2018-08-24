package org.credentialengine.cer.gremlin

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.structure.VertexProperty

class GraphPayloadParser(
        traversal: GraphSourcePool,
        relationships: Relationships,
        val jsonParent: JsonObject,
        val json: JsonObject,
        val contexts: JsonContexts) : PayloadParser(traversal, relationships) {
    private val logger = KotlinLogging.logger {}

    override fun doParse() {
        val id = extractId(json)
        val type = extractType(json)
        logger.info { "Parsing inner @graph object $id." }
        parseDocument(json, id, type)
    }

    override fun parseDocument(json: JsonObject,
                               id: String,
                               type: String) {
        var v = findOrCreateV(id, type).property(
                    VertexProperty.Cardinality.single,
                    Constants.PAYLOAD_PROPERTY, compress(json.toString()))

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
                        v = v.property(VertexProperty.Cardinality.list, key, getPrimitive(itemEntry.asJsonPrimitive))
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

        v.next()
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
        return key != "ceterms:ctid" && contexts.isRefKey(jsonParent["@context"].asString, key)
    }
}
