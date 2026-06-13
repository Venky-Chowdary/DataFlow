# DataFlow Design System v3

Enterprise compact density. Inspired by Fluent UI / Material compact modes.

## Logo semantics

```
  ●━━━━◆━━━━●
 source  gate  dest
 orange       mint
```

| Element | Meaning |
|---------|---------|
| Orange node | Source endpoint |
| Orange path | Outbound extract |
| Diamond gate | 8-gate preflight checkpoint |
| Mint path | Inbound load |
| Mint node | Destination endpoint |

## Size system (4px grid)

| Token | Value | Use |
|-------|-------|-----|
| Control height | 32px | Buttons, inputs, selects |
| Control sm | 28px | Segmented tabs, compact buttons |
| Rail width | 56px | Icon navigation |
| Top bar | 48px | Page title bar |
| Table row | 36px | Dense data tables |
| Icon | 18px | Navigation icons |

## Colors

| Token | Hex |
|-------|-----|
| Blaze Orange | `#FF4D00` |
| Electric Mint | `#00C98A` |

No blue. Neutrals use Fluent gray ramp.

## Typography

| Scale | Size |
|-------|------|
| xs | 11px — labels, captions |
| sm | 12px — body, table cells |
| base | 13px — default |
| md | 14px — top bar title |
| lg | 16px — emphasis |

## Principles

1. One title per view — top bar only, no duplicate H1
2. All interactive controls are 32px tall
3. Metrics as inline pills, not card grids
4. Sections separated by 1px borders, no shadows
5. Maximum content width 960px
