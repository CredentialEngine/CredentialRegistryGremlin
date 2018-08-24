package org.credentialengine.cer.gremlin

import java.lang.Integer.parseInt

class Config {
    val gremlinUsername: String
        get() = System.getenv("GREMLIN_MASTER_USERNAME") ?: "credentialregistry"
    val gremlinPassword: String
        get() = System.getenv("GREMLIN_MASTER_PASSWORD") ?: "8Zc7NJIMFDSrUIv"
    val gremlinAddress: String
        get() = System.getenv("GREMLIN_MASTER_ADDRESS") ?: "localhost"
    val gremlinPort: Int
        get() = parseInt(System.getenv("GREMLIN_MASTER_PORT") ?: "8183")
    val databaseUrl: String
        get() {
            val psqlAddress = System.getenv("POSTGRESQL_ADDRESS")
            val psqlDb = System.getenv("POSTGRESQL_DATABASE")
            if (psqlAddress != null && psqlDb != null) {
                return "jdbc:postgresql://$psqlAddress/$psqlDb"
            } else {
                return "jdbc:postgresql://localhost/metadataregistry_development"
            }
        }
    val databaseUsername: String
        get() = System.getenv("POSTGRESQL_USERNAME") ?: "rmsaksida"
    val databasePassword: String
        get() = System.getenv("POSTGRESQL_PASSWORD") ?: "rmsaksida"
    val redisUrl: String
        get() = System.getenv("REDIS_URL") ?: "redis://localhost:6379"
}
