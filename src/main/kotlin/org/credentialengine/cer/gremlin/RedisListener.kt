package org.credentialengine.cer.gremlin

import com.google.gson.Gson
import mu.KotlinLogging
import org.credentialengine.cer.gremlin.commands.*
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import java.io.Closeable
import kotlin.concurrent.thread

data class GremlinCerMessage(val command: String, val id: Int)

class RedisListener(val jedisPool: JedisPool, val commandCreator: CommandCreator) : Closeable {
    private val logger = KotlinLogging.logger {}

    @Volatile
    var running = true

    fun listen() {
        startWaitThread()
        startWorkThread()
    }

    private fun startWaitThread() {
        thread {
            var jedis: Jedis? = null

            while (true)
            {
                if (!running) break

                try {
                    jedis = jedisPool.resource

                    // ref. https://redis.io/commands/brpoplpush
                    jedis.brpoplpush("gremlin-cer:waiting", "gremlin-cer:working", 0)
                } finally {
                    if (jedis != null) jedis.close()
                }
            }
        }
    }

    private fun startWorkThread() {
        thread {
            var jedis: Jedis? = null

            while (true)
            {
                if (!running) break

                try {
                    jedis = jedisPool.resource

                    // ref. https://redis.io/commands/brpop
                    val rawMessage = jedis.brpop(0, "gremlin-cer:working")[1]

                    val message = Gson().fromJson<GremlinCerMessage>(rawMessage, GremlinCerMessage::class.java)

                    logger.info { "Received command: ${message.command}." }

                    when (message.command) {
                        "create_indices" -> { commandCreator.create<CreateIndices>().run() }
                        "index_one" -> { commandCreator.create<IndexOne>().run(message.id) }
                        "index_all" -> {  commandCreator.create<IndexAll>().run() }
                        "delete_one" -> {  commandCreator.create<DeleteOne>().run(message.id) }
                    }
                } catch (e: Exception) {
                    logger.error(e) { "There was a problem when processing the command." }
                } finally {
                    if (jedis != null) jedis.close()
                }
            }
        }
    }

    override fun close() {
        running = false
    }
}
