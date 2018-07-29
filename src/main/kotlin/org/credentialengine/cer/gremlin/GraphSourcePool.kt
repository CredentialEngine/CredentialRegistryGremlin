package org.credentialengine.cer.gremlin

import mu.KotlinLogging
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.tinkerpop.gremlin.driver.AuthProperties
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph

class ClusterAndTraversal(val cluster: Cluster, val g: GraphTraversalSource) {}

class ClusterAndTraversalFactory(val configuration: Config) : BasePooledObjectFactory<ClusterAndTraversal>() {
    override fun wrap(obj: ClusterAndTraversal?): PooledObject<ClusterAndTraversal> {
        return DefaultPooledObject(obj)
    }

    override fun create(): ClusterAndTraversal {
        val auth = AuthProperties()
                .with(AuthProperties.Property.USERNAME, configuration.gremlinUsername)
                .with(AuthProperties.Property.PASSWORD, configuration.gremlinPassword)
        val cluster = Cluster.build()
                .addContactPoint(configuration.gremlinAddress)
                .port(configuration.gremlinPort)
                .authProperties(auth)
                .enableSsl(true)
                .create()
        val g = EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(cluster))
        return ClusterAndTraversal(cluster, g)
    }

    override fun destroyObject(p: PooledObject<ClusterAndTraversal>) {
        p.`object`.cluster.close()
        p.`object`.g.close()
    }
}

class GraphSourcePool(configuration: Config) {
    private val logger = KotlinLogging.logger {}

    private val pool: GenericObjectPool<ClusterAndTraversal>

    init {
        val poolConfig = GenericObjectPoolConfig<ClusterAndTraversal>()
        val procs = Runtime.getRuntime().availableProcessors()
        poolConfig.minIdle = procs
        poolConfig.maxTotal = procs+1
        pool = GenericObjectPool<ClusterAndTraversal>(ClusterAndTraversalFactory(configuration), poolConfig)
        pool.preparePool()
    }

    fun borrowSource(): ClusterAndTraversal {
        return pool.borrowObject()
    }

    fun returnSource(g: ClusterAndTraversal, invalidate: Boolean = false) {
        if (invalidate) {
            pool.invalidateObject(g)
        } else {
            pool.returnObject(g)
        }
    }

    fun withSource(fn: (ClusterAndTraversal) -> Unit) {
        val source = pool.borrowObject()
        var sourceIsValid = true
        try {
            fn(source)
        } catch (e: Exception) {
            sourceIsValid = false
            logger.error(e) { "There was a problem while performing an operation with a Gremlin source." }
            pool.invalidateObject(source)
        } finally {
            if (sourceIsValid) pool.returnObject(source)
        }
    }
}
