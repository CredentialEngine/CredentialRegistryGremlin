package org.credentialengine.cer.gremlin

import java.sql.*

class ResultSetWrapper(private val resultSet: ResultSet) {
    fun getString(columnIndex: Int): String {
        return resultSet.getString(columnIndex)
    }

    fun getInt(columnIndex: Int): Int {
        return resultSet.getInt(columnIndex)
    }
}

class ResultSetBatcher<T>(val batchSize: Int, preparedStatement: PreparedStatement, val resultSetMapper: (ResultSetWrapper) -> T) {
    val resultSet = preparedStatement.executeQuery()
    var lastNext = resultSet.next()

    operator fun iterator(): Iterator<Collection<T>> {
        return object : Iterator<Collection<T>> {
            override fun next(): Collection<T> {
                val batch = mutableListOf<T>()

                for (i in 1..batchSize) {
                    if (!lastNext) break
                    batch.add(resultSetMapper(ResultSetWrapper(resultSet)))
                    lastNext = resultSet.next()
                }

                return batch.toList()
            }

            override fun hasNext(): Boolean {
                return lastNext
            }
        }
    }
}
