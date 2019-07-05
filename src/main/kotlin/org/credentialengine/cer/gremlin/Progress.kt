package org.credentialengine.cer.gremlin

class Progress(val total: Int, val step: Int, private val reporter: (current: Int, total: Int, percent: Float) -> Unit) {
    private var reportedSteps = 0

    var current = 0
        set(value) {
            if (value > field) {
                synchronized(this) {
                    field = value
                    report()
                }
            }
        }

    fun increment() {
        current += 1
    }

    private fun report() {
        val steps = current / step

        if (steps > reportedSteps || current == total) {
            val percent = current * 100.0f / total
            reporter(current, total, percent)
            reportedSteps = steps
        }
    }
}
