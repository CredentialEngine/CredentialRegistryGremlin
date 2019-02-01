package org.credentialengine.cer.gremlin.commands

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.`__`.inE
import mu.KotlinLogging
import org.credentialengine.cer.gremlin.*

abstract class Command(
        protected val envelopeDatabase: EnvelopeDatabase,
        protected val sourcePool: GraphSourcePool) {
    private val logger = KotlinLogging.logger {}

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
                } else if (relationships.relationshipsOnly) {
                    t = t.hasLabel(Constants.GENERIC_LABEL)
                }

                t = t.has(Constants.GRAPH_ID_PROPERTY, rel.fromId).`as`("a").V()

                if (rel.toType != null) {
                    t = t.hasLabel(rel.toType)
                } else if (relationships.knownTypes.containsKey(rel.toId)) {
                    t = t.hasLabel(relationships.knownTypes[rel.toId])
                } else if (relationships.relationshipsOnly) {
                    t = t.hasLabel(Constants.GENERIC_LABEL)
                }

                t.has(Constants.GRAPH_ID_PROPERTY, rel.toId).addE(rel.relType).from("a").iterate()

                progress.increment()
            }
        }
    }

    protected fun removeOrphans() {
        sourcePool.withSource { source ->
            logger.info {"Removing orphan generated objects."}

            while (source.g.V().has(Constants.GENERATED_PROPERTY).not(inE()).count().next().toInt() != 0) {
                source.g.V().has(Constants.GENERATED_PROPERTY).not(inE()).drop().iterate()
            }
        }
    }
}
