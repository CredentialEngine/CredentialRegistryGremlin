package org.credentialengine.cer.gremlin

class Progress(val total: Int, val step: Int, private val reporter: (current: Int, total: Int, percent: Float) -> Unit) {
    var current = 0

    fun increment() {
        synchronized(this) {
            current += 1
            if (current % step == 0 || current == total) {
                val percent = current * 100.0f / total
                reporter(current, total, percent)
            }
        }
    }
}
