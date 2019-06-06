package org.credentialengine.cer.gremlin.commands

import com.google.gson.JsonObject
import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*
import java.util.*

class DeleteOne(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool)
            : Command(envelopeDatabase, sourcePool, CommandType.DELETE_ONE) {
    private val logger = KotlinLogging.logger {}

    fun run(envelopeId: Int) {
        deleteEnvelope(envelopeId)
        removeOrphans()
        logger.info {"Done."}
    }
}
