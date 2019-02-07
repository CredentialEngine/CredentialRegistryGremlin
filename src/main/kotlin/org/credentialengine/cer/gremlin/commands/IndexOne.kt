package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*

class IndexOne(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        contexts: JsonContexts) :
            ParseCommand(envelopeDatabase, sourcePool,
                    CommandType.INDEX_ONE,
                    contexts) {
    private val logger = KotlinLogging.logger {}

    fun run(envelopeId: Int) {
        logger.info {"Indexing objects for envelope $envelopeId."}

        val envelope = envelopeDatabase.fetchEnvelope(envelopeId) ?: return

        val relationships = Relationships()
        parseEnvelope(relationships, envelopeId, envelope)
        buildRelationships(relationships)
        removeOrphans()

        logger.info {"Done."}
    }
}
