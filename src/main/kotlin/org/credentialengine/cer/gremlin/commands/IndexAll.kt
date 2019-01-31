package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*

class IndexAll(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        contexts: JsonContexts)
            : ParseCommand(envelopeDatabase, sourcePool, contexts) {
    private val logger = KotlinLogging.logger {}

    fun run() {
        val relationships = Relationships()

        val progress = Progress(envelopeDatabase.getTotalEnvelopes(), 100) { current, total, percent ->
            logger.info {"Parsed $current out of $total envelopes (${String.format("%.2f", percent)}%)."}
        }

        val ids = envelopeDatabase.getAllEnvelopeIds()

        for (id in ids) {
            val envelope = envelopeDatabase.fetchEnvelope(id)
            logger.info {"Parsing envelope $id."}
            parseEnvelope(relationships, id, envelope!!)
            progress.increment()
        }

        buildRelationships(relationships)
        removeOrphans()

        logger.info {"Done."}
    }
}
