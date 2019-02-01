package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*

class BuildRelationships(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        contexts: JsonContexts)
            : ParseCommand(envelopeDatabase, sourcePool, contexts) {
    private val logger = KotlinLogging.logger {}

    override val relationshipsOnly = true

    fun run() {
        val relationships = Relationships()
        relationships.relationshipsOnly = true

        val progress = Progress(envelopeDatabase.getTotalEnvelopes(), 100) { current, total, percent ->
            logger.info {"Parsed $current out of $total envelopes (${String.format("%.2f", percent)}%)."}
        }

        for (id in envelopeDatabase.getAllEnvelopeIds())
        {
            val envelope = envelopeDatabase.fetchEnvelope(id)
            logger.info {"Parsing envelope $id."}
            parseEnvelope(relationships, id, envelope!!)
            progress.increment()
        }

        buildRelationships(relationships)

        logger.info {"Done."}
    }
}
