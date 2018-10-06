package org.credentialengine.cer.gremlin

import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.google.gson.JsonPrimitive
import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.structure.VertexProperty

class ObsoletePayloadParser(
        sourcePool: GraphSourcePool,
        relationships: Relationships,
        val json: JsonObject) : PayloadParser(sourcePool, relationships) {
    private val logger = KotlinLogging.logger {}

    override fun doParse() {
        val id = extractId(json)
        val type = extractType(json)
        parseDocument(json, id, type)
    }

    override fun parseDocument(json: JsonObject,
                               id: String,
                               type: String) {
        var v = findOrCreateV(id, type).property(VertexProperty.Cardinality.single, Constants.PAYLOAD_PROPERTY, compress(json.toString()))
        val literals = mutableListOf<Pair<String, JsonPrimitive>>()

        for (entry in json.entrySet()) {
            val key = entry.key
            val value = entry.value
            val entryType = detect(value)
            when (entryType) {
                EntryType.LITERAL -> {
                    v = v.property(VertexProperty.Cardinality.single, key, getPrimitive(value.asJsonPrimitive))
                }
                EntryType.OBJECT -> {
                    parseRelatedDocument(value.asJsonObject, type, id, key)
                }
                EntryType.REFERENCE -> {
                    val refObj = value.asJsonObject
                    addRelationship(id, key, extractId(refObj), type, extractType(refObj))
                }
                EntryType.ARRAY_OF_OBJECTS -> {
                    for (itemEntry in value.asJsonArray) {
                        if (isReferenceValue(itemEntry)) {
                            val refObj = itemEntry.asJsonObject
                            addRelationship(id, key, extractId(refObj), type, extractType(refObj))
                        } else {
                            parseRelatedDocument(itemEntry.asJsonObject, type, id, key)
                        }
                    }
                }
                EntryType.ARRAY_OF_LITERALS -> {
                    for (itemEntry in value.asJsonArray) {
                        literals.add(Pair(key, getPrimitive(itemEntry.asJsonPrimitive)) as Pair<String, JsonPrimitive>)
                    }
                }
                EntryType.ARRAY_OF_REFERENCES -> {}
                EntryType.NONE -> {}
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

    private fun detect(value: JsonElement): EntryType {
        if (value.isJsonArray) {
            val arr = value.asJsonArray
            if (arr.size() == 0) return EntryType.NONE
            if (arr.all { it.isJsonPrimitive }) return EntryType.ARRAY_OF_LITERALS
            if (arr.all { it.isJsonObject }) return EntryType.ARRAY_OF_OBJECTS
            logger.warn { "Found a mixed or nested array. This is not supported." }
            return EntryType.NONE
        }
        if (value.isJsonObject) {
            if (isReferenceValue(value)) return EntryType.REFERENCE
            return EntryType.OBJECT
        }

        if (value.isJsonPrimitive) {
            return EntryType.LITERAL
        }

        return EntryType.NONE
    }

    private fun isReferenceValue(value: JsonElement): Boolean {
        return value.asJsonObject.has("@id")
    }
}
