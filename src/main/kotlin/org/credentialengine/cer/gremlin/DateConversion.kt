package org.credentialengine.cer.gremlin

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.ZoneId
import java.util.*
import javax.xml.datatype.DatatypeFactory

class DateConversion {
    companion object {
        @JvmStatic
        val SystemZone = ZoneId.systemDefault()

        @JvmStatic
        val dateParser = SimpleDateFormat("yyyy-MM-dd")

        @JvmStatic
        val datatypeFactory = DatatypeFactory.newDefaultInstance()

        @JvmStatic
        fun dateToEpoch(value: String): Long {
            return dateParser.parse(value).time / 1000
        }

        @JvmStatic
        fun datetimeToEpoch(value: String): Long {
            return Instant.parse(value).toEpochMilli() / 1000
        }

        @JvmStatic
        fun durationToEpoch(value: String): Long {
            return datatypeFactory.newDuration(value).getTimeInMillis(Date.from(Instant.EPOCH)) / 1000;
        }

        @JvmStatic
        fun timestampToEpoch(value: Timestamp): Long {
            return value.toLocalDateTime().atZone(SystemZone).toEpochSecond();
        }
    }
}
