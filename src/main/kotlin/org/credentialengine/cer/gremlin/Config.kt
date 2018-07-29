package org.credentialengine.cer.gremlin

import java.lang.Integer.parseInt

class Config {
    val gremlinUsername: String
        get() = System.getenv("GREMLIN_USERNAME") ?: "credentialregistry"
    val gremlinPassword: String
        get() = System.getenv("GREMLIN_PASSWORD") ?: "8Zc7NJIMFDSrUIv"
    val gremlinAddress: String
        get() = System.getenv("GREMLIN_ADDRESS") ?: "localhost"
    val gremlinPort: Int
        get() = parseInt(System.getenv("GREMLIN_PORT") ?: "8183")
    val databaseUrl: String
        get() = System.getenv("DATABASE_URL") ?: "jdbc:postgresql://localhost/metadataregistry_development"
    val databaseUsername: String
        get() = System.getenv("DATABASE_USERNAME") ?: "rmsaksida"
    val databasePassword: String
        get() = System.getenv("DATABASE_PASSWORD") ?: "rmsaksida"
    val redisUrl: String
        get() = System.getenv("REDIS_URL") ?: "redis://localhost:6379"
}
