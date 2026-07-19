# Service package compatibility

`apps/api/services` is the canonical package for bare `services.*` imports.
`apps/api/src/services` remains a deliberately thin compatibility namespace for
`src.services.*` callers; its modules re-export the canonical implementations.

Do not add new implementations under `src/services`. New service modules belong
in `services`, with a compatibility re-export added only when an existing
`src.services` caller needs it. The package split remains until imports can be
consolidated through a planned, separately verified migration.
