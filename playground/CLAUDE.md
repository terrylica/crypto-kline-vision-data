# Playground Directory

Experimental prototypes and research spikes. **Not production code.**

**Hub**: [Root CLAUDE.md](../CLAUDE.md) | **Siblings**: [src/](../src/CLAUDE.md) | [tests/](../tests/CLAUDE.md) | [docs/](../docs/CLAUDE.md) | [examples/](../examples/CLAUDE.md) | [scripts/](../scripts/CLAUDE.md)

---

## Subdirectories

| Directory                                                         | Purpose                           |
| ----------------------------------------------------------------- | --------------------------------- |
| [`bybit/`](bybit/CLAUDE.md)                                       | Bybit exchange API exploration    |
| [`fsspec_benchmark/`](fsspec_benchmark/CLAUDE.md)                 | fsspec performance benchmarking   |
| [`fsspec_remote_path_match/`](fsspec_remote_path_match/CLAUDE.md) | fsspec remote path matching tests |
| `funding_rate/`                                                   | Funding rate data experiments     |
| `logger_demo/`                                                    | Logging configuration prototypes  |
| `okx/`                                                            | OKX exchange API exploration      |
| [`rate_limit_test/`](rate_limit_test/CLAUDE.md)                   | Rate limit behavior testing       |
| `vision_checksum/`                                                | Vision API checksum verification  |

---

## Guidelines

1. **Throwaway code** — playground scripts are not maintained or tested
2. **No imports from playground** — production code must never depend on playground
3. **Document findings** — if a prototype validates an approach, move findings to `docs/` or create an ADR
4. **Use CKVD imports** — even prototypes should use `from ckvd import ...` for consistency

---

## Related

- @examples/ - Maintained, documented examples (production-quality)
- @docs/research/ - Research notes (git-ignored)
