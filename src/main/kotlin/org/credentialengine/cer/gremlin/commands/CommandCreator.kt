package org.credentialengine.cer.gremlin.commands

import org.koin.standalone.KoinComponent
import org.koin.standalone.get

class CommandCreator : KoinComponent {
    inline fun <reified T : Command> create(): T {
        return get()
    }
}
