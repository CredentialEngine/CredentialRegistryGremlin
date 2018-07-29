package org.credentialengine.cer.gremlin

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import java.sql.Connection

data class JsonContext(val url: String, val jsonObject: JsonObject)
data class JsonSchema(val id: Int, val name: String, val jsonObject: JsonObject)

class EnvelopeDatabase(val dataSource: HikariDataSource) {
    private val logger = KotlinLogging.logger {}

    fun fetchEnvelope(envelopeId: Int): JsonObject? {
        val query = "SELECT processed_resource FROM envelopes WHERE id = ?"
        val statement = getConnection().prepareStatement(query)
        statement.setInt(1, envelopeId)
        var rs = statement.executeQuery()

        if (!rs.next()) {
            logger.info {"Could not find envelope $envelopeId."}
            return null
        }

        return JsonParser().parse(rs.getString(1)).asJsonObject
    }

    fun getAllEnvelopes(): ResultSetBatcher<Pair<Int, JsonObject>> {
        val query = "SELECT id, processed_resource FROM envelopes WHERE deleted_at IS NULL ORDER BY id"
        val statement = getConnection().prepareStatement(query)
        val batches = ResultSetBatcher(10, statement) { it ->
            Pair(it.getInt(1), JsonParser().parse(it.getString(2)).asJsonObject)
        }
        return batches
    }

    fun getTotalEnvelopes(): Int {
        val totalSql = "SELECT COUNT(*) FROM envelopes WHERE deleted_at IS NULL"
        val totalStatement = getConnection().prepareStatement(totalSql)
        val rs = totalStatement.executeQuery()
        rs.next()
        return rs.getInt(1)
    }

    fun getContexts(): List<JsonContext> {
        val contexts = mutableListOf<JsonContext>()

        val rs = getConnection()
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

    fun getSchemas(): List<JsonSchema> {
        val schemas = mutableListOf<JsonSchema>()
        val rs = getConnection()
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

    fun getConnection(): Connection {
        return dataSource.connection
    }
}
