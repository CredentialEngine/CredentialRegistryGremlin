package org.credentialengine.cer.gremlin

import java.util.concurrent.ConcurrentHashMap

data class Relationship(val fromId: String, val relType: String, val toId: String, val fromType: String?, val toType: String?)

class Relationships {
    var relationshipsOnly = false

    private val relationships = ConcurrentHashMap<Relationship, Boolean>()

    val knownTypes = ConcurrentHashMap<String, String>()

    fun addRelationship(relationship: Relationship) {
        relationships[relationship] = true
    }

    fun registerType(id: String, type: String) {
        knownTypes[id] = type
    }

    fun finalise(): List<Relationship> {
        return relationships.keys().toList()
    }
}
