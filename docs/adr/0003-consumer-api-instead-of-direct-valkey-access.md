# Expose a consumer API instead of Valkey

Queue consumers will retrieve and complete work through a hookrouter HTTP API with long polling rather than connecting directly to Valkey. This keeps the storage schema and credentials internal, gives hookrouter ownership of ordering, leases, retries, dead-letter handling, authentication, and observability, and allows the persistence model to change without breaking consumers. Equivalent consumers compete for work from one shared pool instead of being assigned to recipients.
