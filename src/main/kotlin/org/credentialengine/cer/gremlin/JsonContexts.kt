package org.credentialengine.cer.gremlin

class JsonContexts(val envelopeDatabase: EnvelopeDatabase) {
    private val contextRefs = hashMapOf<String, HashSet<String>>()

    init {
        updateContexts()
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
            for (entry in context.jsonObject.entrySet()) {
                if (entry.value.isJsonObject) {
                    val typeRef = entry.value.asJsonObject
                    if (typeRef.has("@type") && typeRef["@type"].asString == "@id") {
                        refs.add(entry.key)
                    }
                }
            }
        }
    }
}
