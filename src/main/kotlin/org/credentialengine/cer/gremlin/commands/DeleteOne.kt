package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.credentialengine.cer.gremlin.Constants
import org.credentialengine.cer.gremlin.EnvelopeDatabase
import org.credentialengine.cer.gremlin.GraphPayloadParser
import org.credentialengine.cer.gremlin.GraphSourcePool

class DeleteOne(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool)
            : Command(envelopeDatabase, sourcePool) {
    private val logger = KotlinLogging.logger {}

    fun run(envelopeId: Int) {
        logger.info {"Deleting objects for envelope $envelopeId."}

        val toBeDeleted = mutableSetOf<String>()
        val envelope = envelopeDatabase.fetchEnvelope(envelopeId) ?: return
        val json = envelope.processedResource

        if (json.has("@graph")) {
            for (obj in json["@graph"].asJsonArray) {
                val innerJson = obj.asJsonObject
                if (innerJson.has("@id")) {
                    val id = GraphPayloadParser.extractId(innerJson)
                    if (!id.startsWith(Constants.GENERATED_PREFIX)) {
                        toBeDeleted.add(id)
                    }
                }
            }
        } else {
            val id = GraphPayloadParser.extractId(json)
            if (!id.startsWith(Constants.GENERATED_PREFIX)) {
                toBeDeleted.add(id)
            }
        }

        sourcePool.withSource { source ->
            for (id in toBeDeleted) {
                logger.info {"Deleting object ID $id."}
                source.g.V().has(Constants.GRAPH_ID_PROPERTY, id).drop().iterate()
            }
        }

        removeOrphans()

        logger.info {"Done."}
    }
}
