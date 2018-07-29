package org.credentialengine.cer.gremlin.commands

import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.credentialengine.cer.gremlin.Constants
import org.credentialengine.cer.gremlin.EnvelopeDatabase
import org.credentialengine.cer.gremlin.GraphSourcePool

class CreateIndices(
        envelopeDatabase: EnvelopeDatabase,
        sourcePool: GraphSourcePool)
    : Command(envelopeDatabase, sourcePool) {
    private val logger = KotlinLogging.logger {}

    fun run() {
        logger.info {"Creating indices using JSON schemas."}

        val indices = mutableSetOf<String>()

        for (schema in envelopeDatabase.getSchemas()) {
            val asJsonObj = schema.jsonObject.asJsonObject
            if (!asJsonObj.has("definitions")) continue
            for (definition in asJsonObj["definitions"].asJsonObject.entrySet()) {
                val definitionObj = definition.value.asJsonObject
                if (definitionObj.has("type") && definitionObj["type"].asString == "object") {
                    indices.add(definition.key)
                }
            }
        }

        sourcePool.withSource { source ->
            for (indexName in indices) {
                buildIndex(source.cluster, indexName)
            }
            buildIndex(source.cluster, Constants.GENERIC_LABEL)
        }
    }

    private fun buildIndex(cluster: Cluster, label: String) {
        logger.info {"Creating Neo4J indices for $label."}

        val createIndex = """
            !graph.tx().isOpen() && graph.tx().open();
            graph.cypher("CREATE INDEX ON :`$label`(`${Constants.GRAPH_ID_PROPERTY}`)");
            graph.tx().commit()
        """.trimIndent()
        cluster.connect<Client>().submit(createIndex).all().get()
    }
}
