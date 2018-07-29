package org.credentialengine.cer.gremlin

class JsonContexts(envelopeDatabase: EnvelopeDatabase) {
    private val contexts = envelopeDatabase.getContexts()

    private val contextRefs = hashMapOf<String, HashSet<String>>()

    fun isRefKey(url: String, key: String): Boolean {
        val hashSet = contextRefs[url]
        return hashSet?.contains(key) ?: false
    }

    init {
        for (context in contexts) {
            var refs = contextRefs[context.url]
            if (refs == null) {
                refs = hashSetOf()
                contextRefs[context.url] = refs
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
