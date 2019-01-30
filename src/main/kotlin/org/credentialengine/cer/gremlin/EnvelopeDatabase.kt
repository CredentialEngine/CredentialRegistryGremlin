package org.credentialengine.cer.gremlin

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import com.zaxxer.hikari.HikariDataSource
import mu.KotlinLogging
import java.sql.Timestamp
import java.time.ZoneId

data class JsonContext(val url: String, val jsonObject: JsonObject)
data class JsonSchema(val id: Int, val name: String, val jsonObject: JsonObject)
data class Envelope(val id: Int, val processedResource: JsonObject, val createdAt: Long, val updatedAt: Long, val ctid: String) {
    val context: String = processedResource.get("@context").asString
}

class EnvelopeDatabase(val dataSource: HikariDataSource) {
    private val logger = KotlinLogging.logger {}

    fun fetchEnvelope(envelopeId: Int): Envelope? {
        var envelope: Envelope? = null

        dataSource.connection.use { con ->
            val query = "SELECT created_at, updated_at, processed_resource, envelope_ceterms_ctid FROM envelopes WHERE id = ?"
            con.prepareStatement(query).use { sta ->
                sta.setInt(1, envelopeId)
                sta.executeQuery().use { rs ->
                    if (rs.next()) {
                        envelope = Envelope(
                                envelopeId,
                                JsonParser().parse(rs.getString(3)).asJsonObject,
                                DateConversion.timestampToEpoch(rs.getTimestamp(1)),
                                DateConversion.timestampToEpoch(rs.getTimestamp(2)),
                                rs.getString(4))
                    } else {
                        logger.info {"Could not find envelope $envelopeId."}
                    }
                }
            }
        }

        if (envelope != null) {
            return envelope
        }

        return null
    }

    fun getAllEnvelopeIds(): List<Int> {
        val ids = mutableListOf<Int>()

        dataSource.connection.use { con ->
            con.prepareStatement("" +
                    "SELECT id " +
                    "FROM envelopes " +
                    "WHERE deleted_at IS NULL " +
                    "AND (processed_resource->'@graph') IS NOT NULL " +
                    "ORDER BY id").use { sta ->
                sta.executeQuery().use { rs ->
                    while (rs.next())
                    {
                        ids.add(rs.getInt(1))
                    }
                }
            }
        }

        return ids.toList()
    }

    fun getTotalEnvelopes(): Int {
        var total = 0

        dataSource.connection.use { con ->
            val totalSql = "SELECT COUNT(*) FROM envelopes WHERE deleted_at IS NULL"
            con.prepareStatement(totalSql).use { sta ->
                sta.executeQuery().use { rs ->
                    rs.next()
                    total = rs.getInt(1)
                }
            }

        }

        return total
    }

    fun getContexts(): List<JsonContext> {
        val contexts = mutableListOf<JsonContext>()

        dataSource.connection.use { con ->
            con.prepareStatement("SELECT url, context FROM json_contexts").use { sta ->
                sta.executeQuery().use { rs ->
                    while (rs.next())
                    {
                        val url = rs.getString(1)
                        val context = JsonParser().parse(rs.getString(2)).asJsonObject.get("@context").asJsonObject
                        contexts.add(JsonContext(url, context))
                    }
                }
            }
        }

        return contexts.toList()
    }

    fun getSchemas(): List<JsonSchema> {
        val schemas = mutableListOf<JsonSchema>()

        dataSource.connection.use { con ->
            con.prepareStatement("SELECT id, NAME, SCHEMA FROM json_schemas").use { sta ->
                sta.executeQuery().use { rs ->
                    while (rs.next())
                    {
                        val id = rs.getInt(1)
                        val name = rs.getString(2)
                        val schema = JsonParser().parse(rs.getString(3)).asJsonObject
                        schemas.add(JsonSchema(id, name, schema))
                    }
                }
            }
        }

        return schemas.toList()
    }

    fun updateIndexTime(envelopeId: Int): Boolean {
        var updated = false

        dataSource.connection.use { con ->
            con.prepareStatement("UPDATE envelopes SET last_graph_indexed_at = ? WHERE id = ?").use { sta ->
                sta.setTimestamp(1, Timestamp(System.currentTimeMillis()))
                sta.setInt(2, envelopeId)
                updated = sta.execute()
            }
        }

        return updated
    }
}
