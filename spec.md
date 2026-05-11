# DEPRECATED

This file (`spec.md`) is a superseded draft. The canonical technical specification is `spec-final.md`.

Key differences in the final spec:
- **Runtime**: Bun-only (not Bun/Node)
- **Memory pool**: Fixed-size boxes with pool strategy (no SM/MD/LG size classes)
- **Resource params**: Named params via Zod objects (not positional)
- **Supervision**: Root group supervises all actors by default
- **Diagnostics**: Configurable per-metric with sampling

Please refer to `spec-final.md` for the authoritative specification.
