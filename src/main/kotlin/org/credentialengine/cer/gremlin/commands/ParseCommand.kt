package org.credentialengine.cer.gremlin.commands

import com.google.gson.JsonObject
import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*
import java.util.concurrent.atomic.AtomicInteger

abstract class ParseCommand(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        val contexts: JsonContexts)
            : Command(envelopeDatabase, sourcePool) {
    private val logger = KotlinLogging.logger {}

    protected fun parseEnvelope(relationships: Relationships, id: Int, json: JsonObject) {
        if (json.has("@graph")) {
            parseFromGraph(json, relationships, id)
        } else {
            parseFromParent(relationships, json, id)
        }
    }

    fun parseFromParent(relationships: Relationships, json: JsonObject, id: Int) {
        val parser = ObsoletePayloadParser(sourcePool, relationships, json)

        try {
            parser.use { it -> it.parse() }
            logger.debug { "Updating document index time." }
            envelopeDatabase.updateIndexTime(id)
        } catch (e: Exception) {
            // When there's an error in graph interaction, we get a meaningless exception and
            // all subsequent interactions fail until the pool is reestablished.
            // Instead let's create another pool.
            logger.error(e) { "There was a problem when parsing the document." }
        }
    }

    fun parseFromGraph(json: JsonObject, relationships: Relationships, id: Int) {
        val graphItems = json["@graph"].asJsonArray.toList()
        var parsedCount = 0
        for (graphJson in graphItems) {
            val parser = GraphPayloadParser(
                    sourcePool,
                    relationships,
                    json,
                    graphJson.asJsonObject,
                    contexts)
            try {
                parser.use { it -> it.parse() }
                parsedCount += 1
            } catch (e: Exception) {
                // When there's an error in graph interaction, we get a meaningless exception and
                // all subsequent interactions fail until the pool is reestablished.
                // Instead let's create another pool.
                logger.error(e) { "There was a problem when parsing the document." }
            }
        }
        if (parsedCount == graphItems.count()) {
            logger.debug { "Updating document index time." }
            envelopeDatabase.updateIndexTime(id)
        }
    }
}
