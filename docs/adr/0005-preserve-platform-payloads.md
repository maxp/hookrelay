# Preserve complete platform-specific payloads

Canonical Messages will use a small common envelope while preserving the complete platform-specific JSON payload without cross-platform normalization or removal of unknown fields. Consumers require details that a converter may not understand, and verified valid payloads with unknown structure are routed to a relay-scoped Recipient rather than discarded. Exact original JSON bytes, whitespace, and object-key order are not part of the Canonical Payload contract.
