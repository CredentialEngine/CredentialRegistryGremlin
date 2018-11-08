package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.credentialengine.cer.gremlin.EnvelopeDatabase
import org.credentialengine.cer.gremlin.GraphSourcePool
import org.credentialengine.cer.gremlin.JsonContexts
import org.credentialengine.cer.gremlin.Relationships

class IndexOne(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        contexts: JsonContexts) :
            ParseCommand(envelopeDatabase, sourcePool, contexts) {
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
