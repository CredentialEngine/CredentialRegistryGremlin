package org.credentialengine.cer.gremlin

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneId
import java.util.*
import javax.xml.datatype.DatatypeFactory

class DateConversion {
    companion object {
        val SystemZone = ZoneId.systemDefault()
        val dateParser = SimpleDateFormat("yyyy-MM-dd")
        val datetimeParser = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
        val datatypeFactory = DatatypeFactory.newDefaultInstance()

        fun dateToEpoch(value: String): Long {
            return dateParser.parse(value).time / 1000
        }

        fun datetimeToEpoch(value: String): Long {
            return datetimeParser.parse(value).time / 1000
        }

        fun durationToEpoch(value: String): Long {
            return datatypeFactory.newDuration(value).getTimeInMillis(Date.from(Instant.EPOCH)) / 1000;
        }

        fun timestampToEpoch(value: Timestamp): Long {
            return value.toLocalDateTime().atZone(SystemZone).toEpochSecond();
        }
    }
}
