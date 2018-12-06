# Gremlin indexing service for the Credential Registry (gremlin-cer)

This is a background service that runs in parallel to the Credential Registry application server and database. It is able to connect to the CER DB, fetch envelopes and index them in a Gremlin server.

The service can be configured by setting environment variables. The full list is available in [Config.kt](src/main/kotlin/org/credentialengine/cer/gremlin/Config.kt).

Additionally, service can be remotely controlled via JSON-encoded messages pushed to a Redis queue. The available messages are:

- `{ "command": "create_indices" }`
- `{ "command": "index_one", "id": [id] }`
- `{ "command": "index_all" }`
- `{ "command": "delete_one", "id": [id] }`
- `{ "command": "update_contexts" }`
