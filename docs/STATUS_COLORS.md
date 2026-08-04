# Status color system (safe / review / risk)

Studio severity language must stay consistent across Map, Validate, Jobs, and dark mode.

| Token intent | Meaning | Typical use |
|--------------|---------|-------------|
| **Safe / ready** | Operator-cleared or engine-passed | Approved mappings, passed gates, successful jobs |
| **Review** | Needs human attention; not yet a hard lossy risk | Ambiguous maps, soft constraint hints, advisor chips |
| **Risk / block** | Fail-closed or explicit lossy/mutate path | G4 risk ack required, G9 encoding block, critical Validate CTAs |

## Rules

1. **Never** paint a blocked gate green.
2. Soft advisories (Snowflake warehouse size, constraint hints) use **review** tone — not block red — unless they escalate to a hard gate.
3. Risk ack / Accept-risk actions use **risk** tone distinct from Review.
4. Dark mode must preserve contrast for safe/review/risk — do not invert severity into decorative pastels.

CSS anchors live under `.df2-*` Studio classes (`transfer-studio.css`, design tokens). This doc is the human-factors SSOT for product/design review.
