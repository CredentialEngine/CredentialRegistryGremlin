package org.credentialengine.cer.gremlin

import java.util.concurrent.ConcurrentHashMap

data class Relationship(val fromId: String, val relType: String, val toId: String, val fromType: String?, val toType: String?)

class Relationships {
    private val relationships = ConcurrentHashMap<Relationship, Boolean>()

    fun addRelationship(relationship: Relationship) {
        relationships[relationship] = true
    }

    fun finalise(): List<Relationship> {
        return relationships.keys().toList()
    }
}
