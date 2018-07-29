package org.credentialengine.cer.gremlin.commands

import com.google.gson.JsonObject
import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*

abstract class ParseCommand(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        val contexts: JsonContexts)
            : Command(envelopeDatabase, sourcePool) {
    private val logger = KotlinLogging.logger {}

    protected fun parseEnvelope(relationships: Relationships, id: Int, json: JsonObject) {
        val parser = if (json.has("@graph"))
            GraphPayloadParser(sourcePool, relationships, json, contexts)
        else
            ObsoletePayloadParser(sourcePool, relationships, json)

        try {
            parser.use { it -> it.parse() }
        } catch (e: Exception) {
            // When there's an error in graph interaction, we get a meaningless exception and
            // all subsequent interactions fail until the pool is reestablished.
            // Instead let's create another pool.
            logger.error(e) { "There was a problem when parsing the document." }
        }
    }
}
