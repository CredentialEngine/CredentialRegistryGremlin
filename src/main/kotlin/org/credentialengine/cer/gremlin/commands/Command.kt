package org.credentialengine.cer.gremlin.commands

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.inE
import mu.KotlinLogging
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.credentialengine.cer.gremlin.*

const val ORPHAN_BATCH_SIZE = 10_000

abstract class Command(
        protected val envelopeDatabase: EnvelopeDatabase,
        protected val sourcePool: GraphSourcePool,
        protected val commandType: CommandType) {
    private val logger = KotlinLogging.logger {}

    private val orphans: GraphTraversal<Vertex, Vertex>
        get() {
            var traversal: GraphTraversal<Vertex, Vertex>? = null

            sourcePool.withSource { source ->
                traversal = source.g.V().has(Constants.GENERATED_PROPERTY).not(inE())
            }

            return traversal!!
        }

    private val orphansCount get() = orphans.count().next().toInt()

    protected fun buildRelationships(relationships: Relationships) {
        val rels = relationships.finalise()

        logger.info {"Building ${rels.size} relationships."}
        val progress = Progress(rels.size, 100) { cur, total, pct ->
            logger.info { "Built $cur out of $total relationships (${String.format("%.2f", pct)}%)." }
        }

        rels.parallelStream().forEach { rel ->
            sourcePool.withSource { source ->
                var t = source.g.V()

                if (rel.fromType != null) {
                    t = t.hasLabel(rel.fromType)
                } else if (relationships.knownTypes.containsKey(rel.fromId)) {
                    t = t.hasLabel(relationships.knownTypes[rel.fromId])
                } else if (commandType == CommandType.BUILD_RELATIONSHIPS || commandType == CommandType.INDEX_ALL) {
                    t = t.hasLabel(Constants.GENERIC_LABEL)
                }

                t = t.has(Constants.GRAPH_ID_PROPERTY, rel.fromId).`as`("a").V()

                if (rel.toType != null) {
                    t = t.hasLabel(rel.toType)
                } else if (relationships.knownTypes.containsKey(rel.toId)) {
                    t = t.hasLabel(relationships.knownTypes[rel.toId])
                } else if (commandType == CommandType.BUILD_RELATIONSHIPS || commandType == CommandType.INDEX_ALL) {
                    t = t.hasLabel(Constants.GENERIC_LABEL)
                }

                t.has(Constants.GRAPH_ID_PROPERTY, rel.toId).addE(rel.relType).from("a").iterate()

                progress.increment()
            }
        }
    }

    protected fun deleteEnvelope(envelopeId: Int) {
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
    }

    protected fun removeOrphans() {
        val total = orphansCount

        logger.info { "Removing $total orphan generated objects." }

        val progress = Progress(total, ORPHAN_BATCH_SIZE) { cur, _, pct ->
            logger.info { "Removed $cur out of $total orphan generated objects (${String.format("%.2f", pct)}%)." }
        }

        while (true) {
            orphans.limit(ORPHAN_BATCH_SIZE.toLong()).drop().iterate()

            val remaining = orphansCount
            progress.current = total - remaining

            if (remaining == 0) {
                break
            }
        }
    }
}
