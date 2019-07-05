package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*

class RemoveOrphans(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool,
        contexts: JsonContexts)
    : ParseCommand(envelopeDatabase, sourcePool, CommandType.REMOVE_ORPHANS, contexts) {
    private val logger = KotlinLogging.logger {}

    fun run() {
        removeOrphans()
        logger.info {"Done."}
    }
}
