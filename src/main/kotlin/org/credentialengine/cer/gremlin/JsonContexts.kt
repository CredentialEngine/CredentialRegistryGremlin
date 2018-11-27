package org.credentialengine.cer.gremlin

import org.apache.tinkerpop.gremlin.structure.Graph
import java.io.Console

data class GraphPrimitive(
        val transformed: Boolean,
        val original: Any?,
        val new: Any?, val
        suffix: String?)

class ContextTypes {
    private val contextTypes = hashMapOf<String, String>()
    fun clear() {
        contextTypes.clear()
    }

    fun add(key: String, type: String) {
        contextTypes[key] = type
    }

    fun convertToPrimitive(key: String, str: String?): GraphPrimitive {
        if (str == null || !contextTypes.containsKey(key)) {
            return GraphPrimitive(false, str, null, null)
        }

        val type = contextTypes[key]
        if (type == "xsd:date") {
            return GraphPrimitive(true, str, DateConversion.dateToEpoch(str), "integer")
        } else if (type == "xsd:dateTime") {
            return GraphPrimitive(true, str, DateConversion.datetimeToEpoch(str), "integer")
        } else if (type == "xsd:duration") {
            return GraphPrimitive(true, str, DateConversion.durationToEpoch(str), "integer")
        }

        return GraphPrimitive(false, str, null, null)
    }
}

class JsonContexts(val envelopeDatabase: EnvelopeDatabase) {
    private val contextRefs = hashMapOf<String, HashSet<String>>()
    private val contextTypes = hashMapOf<String, ContextTypes>()

    init {
        updateContexts()
    }

    fun getContextTypes(url: String): ContextTypes? {
        return contextTypes[url]
    }

    fun isRefKey(url: String, key: String): Boolean {
        val hashSet = contextRefs[url]
        return hashSet?.contains(key) ?: false
    }

    fun updateContexts() {
        for (context in envelopeDatabase.getContexts()) {
            var refs = contextRefs[context.url]
            if (refs == null) {
                refs = hashSetOf()
                contextRefs[context.url] = refs
            } else {
                refs.clear()
            }

            var types = contextTypes[context.url]
            if (types == null) {
                types = ContextTypes()
                contextTypes[context.url] = types
            } else {
                types.clear()
            }

            for (entry in context.jsonObject.entrySet()) {
                if (entry.value.isJsonObject) {
                    val typeRef = entry.value.asJsonObject
                    if (typeRef.has("@type")) {
                        val type = typeRef["@type"].asString
                        if (type == "@id") {
                            refs.add(entry.key)
                        }
                        types.add(entry.key, type)
                    }
                }
            }
        }
    }
}
