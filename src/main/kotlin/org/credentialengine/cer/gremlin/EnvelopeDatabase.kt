package org.credentialengine.cer.gremlin

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import java.sql.Connection
import java.sql.Timestamp

data class JsonContext(val url: String, val jsonObject: JsonObject)
data class JsonSchema(val id: Int, val name: String, val jsonObject: JsonObject)

class EnvelopeDatabase(val dataSource: HikariDataSource) {
    private val logger = KotlinLogging.logger {}

    fun fetchEnvelope(envelopeId: Int): JsonObject? {
        connection.use {
            val query = "SELECT processed_resource FROM envelopes WHERE id = ?"
            val statement = it.prepareStatement(query)
            statement.setInt(1, envelopeId)
            var rs = statement.executeQuery()

            if (!rs.next()) {
                logger.info {"Could not find envelope $envelopeId."}
                return null
            }

            return JsonParser().parse(rs.getString(1)).asJsonObject
        }
    }

    fun getAllEnvelopeIds(): List<Int> {
        connection.use {
            val ids = mutableListOf<Int>()

            val rs = it
                    .prepareStatement("SELECT id FROM envelopes WHERE deleted_at IS NULL ORDER BY id")
                    .executeQuery()

            while (rs.next())
            {
                ids.add(rs.getInt(1))
            }

            return ids.toList()
        }
    }

    fun getTotalEnvelopes(): Int {
        connection.use {
            val totalSql = "SELECT COUNT(*) FROM envelopes WHERE deleted_at IS NULL"
            val totalStatement = it.prepareStatement(totalSql)
            val rs = totalStatement.executeQuery()
            rs.next()
            return rs.getInt(1)
        }
    }

    fun getContexts(): List<JsonContext> {
        connection.use {
            val contexts = mutableListOf<JsonContext>()

            val rs = it
                    .prepareStatement("SELECT url, context FROM json_contexts")
                    .executeQuery()

            while (rs.next())
            {
                val url = rs.getString(1)
                val context = JsonParser().parse(rs.getString(2)).asJsonObject
                contexts.add(JsonContext(url, context))
            }

            return contexts.toList()
        }
    }

    fun getSchemas(): List<JsonSchema> {
        connection.use {
            val schemas = mutableListOf<JsonSchema>()
            val rs = it
                    .prepareStatement("SELECT id, NAME, SCHEMA FROM json_schemas")
                    .executeQuery()

            while (rs.next())
            {
                val id = rs.getInt(1)
                val name = rs.getString(2)
                val schema = JsonParser().parse(rs.getString(3)).asJsonObject
                schemas.add(JsonSchema(id, name, schema))
            }

            return schemas.toList()
        }
    }

    fun updateIndexTime(envelopeId: Int): Boolean {
        connection.use {
            val statement = it.prepareStatement("UPDATE envelopes SET last_graph_indexed_at = ? WHERE id = ?")
            statement.setTimestamp(1, Timestamp(System.currentTimeMillis()))
            statement.setInt(2, envelopeId)
            return statement.execute()
        }
    }

    private val connection: Connection
        get() = dataSource.connection
}
