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

    protected open val relationshipsOnly = false

    protected fun parseEnvelope(relationships: Relationships, id: Int, envelope: Envelope) {
        if (envelope.processedResource.has("@graph")) {
            parseFromGraph(relationships, envelope, id)
        } else {
            logger.error { "Asked to parse envelope with no graph." }
        }
    }

    fun parseFromGraph(relationships: Relationships, envelope: Envelope, id: Int) {
        val graphItems = envelope.processedResource["@graph"].asJsonArray.toList()
        var parsedCount = 0
        for (graphJson in graphItems) {
            val parser = GraphPayloadParser(
                    sourcePool,
                    relationships,
                    relationshipsOnly,
                    envelope,
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
        if (parsedCount == graphItems.count() && !relationshipsOnly) {
            logger.debug { "Updating document index time." }
            envelopeDatabase.updateIndexTime(id)
        }
    }
}
