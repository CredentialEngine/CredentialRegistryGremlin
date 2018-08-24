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

        envelopeDatabase.getAllEnvelopes().use { batches ->
            for (batch in batches)
            {
                for (env in batch) {
                    logger.info {"Parsing envelope ${env.first}."}
                    parseEnvelope(relationships, env.first, env.second)
                    progress.increment()
                }
            }
        }

        buildRelationships(relationships)
        removeOrphans()

        logger.info {"Done."}
    }
}
