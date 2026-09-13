---
type: regex
target: { source: file, path: legacy_pipeline/legacy_pipeline/assets.py }
pattern: "AutoMaterializePolicy|non_argument_deps|SourceAsset\\("
match: not_contains
---
