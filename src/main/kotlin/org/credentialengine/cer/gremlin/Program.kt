package org.credentialengine.cer.gremlin

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import org.credentialengine.cer.gremlin.commands.*
import org.koin.dsl.module.applicationContext
import org.koin.standalone.StandAloneContext.startKoin
import redis.clients.jedis.JedisPool
import kotlin.concurrent.thread

val cerModule = applicationContext {
    bean { Config() }
    bean {
        val appConfig = get<Config>()
        val hikariConfig = HikariConfig()
        hikariConfig.jdbcUrl = appConfig.databaseUrl
        hikariConfig.username = appConfig.databaseUsername
        hikariConfig.password = appConfig.databasePassword
        hikariConfig.minimumIdle = 1
        hikariConfig.maximumPoolSize = 10
        hikariConfig.maxLifetime = 60 * 2 * 1000
        HikariDataSource(hikariConfig)
    }
    bean { EnvelopeDatabase(get()) }
    bean { GraphSourcePool(get()) }
    bean { JsonContexts(get()) }
    bean {
        val appConfig = get<Config>()
        JedisPool(appConfig.redisUrl)
    }
    bean { CommandCreator() }
    bean { RedisListener(get(), get(), get()) }
    factory { CreateIndices(get(), get()) }
    factory { DeleteOne(get(), get()) }
    factory { IndexAll(get(), get(), get()) }
    factory { IndexOne(get(), get(), get()) }
    factory { BuildRelationships(get(), get(), get()) }
}

fun main(args : Array<String>) {
    val logger = KotlinLogging.logger {}
    var context = startKoin(listOf(cerModule))
    val redisListener = context.koinContext.get<RedisListener>()
    val dbPool = context.koinContext.get<HikariDataSource>()

    Runtime.getRuntime().addShutdownHook(thread(false) {
        logger.info { "Shutting down." }
        redisListener.close()
        dbPool.close()
    })

    redisListener.listen()
    logger.info { "Listening to messages" }
}

class Program {

}
