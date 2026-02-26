# Environment Variables & Configuration Docs

Verify all documented environment variables (CKVD_LOG_LEVEL, CKVD_ENABLE_CACHE, CKVD_USE_POLARS_OUTPUT) are actually used in code. Grep source for env var usage: grep -r CKVD_LOG_LEVEL src/ckvd/, grep -r CKVD_ENABLE_CACHE src/ckvd/, grep -r CKVD_USE_POLARS_OUTPUT src/ckvd/. Check config.py for how they're read. Test behavior with invalid values. Broadcast undocumented or non-functional env vars.
