## [2.0.2](https://github.com/terrylica/data-source-manager/compare/v2.0.1...v2.0.2) (2026-02-05)


### Bug Fixes

* **cache:** change scan_ipc to scan_parquet for cache reads ([552eef3](https://github.com/terrylica/data-source-manager/commit/552eef3c19916b053c143a902ed3385903715ca8)), closes [#103](https://github.com/terrylica/data-source-manager/issues/103)
* **polars:** add schema standardization for cache file merging ([f7c355b](https://github.com/terrylica/data-source-manager/commit/f7c355b98dfd528a6793fa75121881f939c23723)), closes [#104](https://github.com/terrylica/data-source-manager/issues/104)


### Performance Improvements

* **polars:** complete Phase 2-3 memory efficiency refactoring ([16fadd8](https://github.com/terrylica/data-source-manager/commit/16fadd8746c7a02a87ee4dd1ce78ac750123fa0f)), closes [#11](https://github.com/terrylica/data-source-manager/issues/11)

## [2.0.1](https://github.com/terrylica/data-source-manager/compare/v2.0.0...v2.0.1) (2026-02-05)


### Performance Improvements

* **memory:** phase 0-1 memory efficiency optimizations ([e3a2b11](https://github.com/terrylica/data-source-manager/commit/e3a2b114e7ae4cf7ecf26e1f6b003d42118719ef)), closes [#13](https://github.com/terrylica/data-source-manager/issues/13) [#11](https://github.com/terrylica/data-source-manager/issues/11)

# [2.0.0](https://github.com/terrylica/data-source-manager/compare/v1.6.0...v2.0.0) (2026-02-05)


### Code Refactoring

* **utils:** eliminate ~1,880 LOC of dead code ([d22e75b](https://github.com/terrylica/data-source-manager/commit/d22e75b9499b315b89428aff6dbc75ffbe3fe46c)), closes [#12](https://github.com/terrylica/data-source-manager/issues/12)


### BREAKING CHANGES

* **utils:** Remove deprecated exports and dead code infrastructure

- Fix unreachable else block in dataframe_utils.py (Vulture 100% confidence)
- Delete utils/for_logger/ directory (9 files, 1,413 LOC) - zero consumers
- Delete utils/dsm_config.py (351 LOC) - unused, DataSourceConfig is canonical
- Delete utils/for_demo/dsm_datetime_parser.py (108 LOC) - duplicate of dsm_date_range_utils
- Delete utils/logger_setup.py (51 LOC) - deprecated re-export with zero consumers
- Remove TimeoutError alias from rest_exceptions.py
- Remove create_legacy_client() from client_factory.py and all re-exports

# [1.6.0](https://github.com/terrylica/data-source-manager/compare/v1.5.0...v1.6.0) (2026-02-05)


### Bug Fixes

* align Vision API interface and remove dead code ([b8c5eae](https://github.com/terrylica/data-source-manager/commit/b8c5eaeca0f07ebd3c2700a2b81ebcc22fa2bc66)), closes [#3-8](https://github.com/terrylica/data-source-manager/issues/3-8) [#7](https://github.com/terrylica/data-source-manager/issues/7) [#5](https://github.com/terrylica/data-source-manager/issues/5)


### Features

* **availability:** wire data availability validation with fail-loud behavior ([11563c5](https://github.com/terrylica/data-source-manager/commit/11563c53897e681325c4375fa506642bcf5383a9))

# [1.5.0](https://github.com/terrylica/data-source-manager/compare/v1.4.0...v1.5.0) (2026-02-05)


### Features

* **providers:** wire factory pattern to DataSourceManager ([1c609ea](https://github.com/terrylica/data-source-manager/commit/1c609eabee75a4e329e3ba4db71482734072e8f6))
* **release:** add local PyPI publishing with 1Password integration ([af489ec](https://github.com/terrylica/data-source-manager/commit/af489ec8fbfbc178b03b81d1097a44dd7a4206b2))
* **release:** include PyPI publish in release:full workflow ([21a6746](https://github.com/terrylica/data-source-manager/commit/21a67469c81e469d060210c65fe08da52818d500))

# [1.4.0](https://github.com/terrylica/data-source-manager/compare/v1.3.0...v1.4.0) (2026-02-04)


### Features

* **memory:** add stress tests and memory optimizations ([1ce1a55](https://github.com/terrylica/data-source-manager/commit/1ce1a55568291144ece31fcf5e54aa0f04e36114)), closes [#1](https://github.com/terrylica/data-source-manager/issues/1)

# [1.3.0](https://github.com/terrylica/data-source-manager/compare/v1.2.0...v1.3.0) (2026-02-04)


### Features

* **dsm:** complete feature phases 4-5 with OKX provider and parallel fetching ([d83dd16](https://github.com/terrylica/data-source-manager/commit/d83dd16f9fb2f93ff4a2a20c992608c6cd82a2e0))

# [1.2.0](https://github.com/terrylica/data-source-manager/compare/v1.1.0...v1.2.0) (2026-02-04)


### Features

* **dsm:** add return_polars parameter and provider validation ([33ba8cc](https://github.com/terrylica/data-source-manager/commit/33ba8ccff7b2a0679d6846b116426e4bc9043187))

# [1.1.0](https://github.com/terrylica/data-source-manager/compare/v1.0.2...v1.1.0) (2026-02-03)


### Features

* **memory:** implement Polars-first memory optimization ([49abd14](https://github.com/terrylica/data-source-manager/commit/49abd140032d5325c3708031e005639087b12004))

## [1.0.2](https://github.com/terrylica/data-source-manager/compare/v1.0.1...v1.0.2) (2026-02-02)


### Bug Fixes

* **types:** resolve pyright type errors (9 errors → 0) ([f4e9123](https://github.com/terrylica/data-source-manager/commit/f4e9123bca02b304664dbc541f0691f4c140b435))

## [1.0.1](https://github.com/terrylica/data-source-manager/compare/v1.0.0...v1.0.1) (2026-02-01)


### Bug Fixes

* **dsm-lib:** replace broad exception with specific types ([35354ff](https://github.com/terrylica/data-source-manager/commit/35354ff71b0c034a202c747cc8d76bff8e48e248))
* **dsm:** add polars-exception comment and simplify ternary ([64d8272](https://github.com/terrylica/data-source-manager/commit/64d82723399311baab77f0f0db7ea8a5b0bc2f8d))
* **dsm:** add return type annotations to public methods ([6199e1e](https://github.com/terrylica/data-source-manager/commit/6199e1e4901287f9df42440a42dd814ef435d5da))
* **examples:** rewrite broken lazy initialization demo and remove redundant example ([bdfebd0](https://github.com/terrylica/data-source-manager/commit/bdfebd025c052d50449228f8c3367916c634cd61))
* **init:** add type annotation to __getattr__ (ANN001/ANN202) ([a86763b](https://github.com/terrylica/data-source-manager/commit/a86763bed079913b6087638f36e1b75b87f780c9))
* **logging:** upgrade error logs from DEBUG to WARNING level ([1cc5752](https://github.com/terrylica/data-source-manager/commit/1cc57524eb4b9da808a993f93753f00ca10b4348))
* **rest-client:** propagate RateLimitError instead of silent failure ([fce8b0f](https://github.com/terrylica/data-source-manager/commit/fce8b0f2b6a2ff917195a214afa8ed399fe1dc4f))
* **rest-metrics:** use UTC-aware datetime for rate limit tracking ([46e98de](https://github.com/terrylica/data-source-manager/commit/46e98decb5a33a5425ff9b9b0c10fd761c7356cf))
* **tests:** replace silent failure patterns with explicit errors ([71e3c09](https://github.com/terrylica/data-source-manager/commit/71e3c09eaf0c5b8dedd4c80991bac16e04c522fd))
* **utils:** add ADR references and modernize type hints ([7b1c146](https://github.com/terrylica/data-source-manager/commit/7b1c146f3d47b78fa8c55f2fce829be095db80cc))
* **utils:** add polars-exception comments and fix bare except ([7e46ac1](https://github.com/terrylica/data-source-manager/commit/7e46ac1b5af121cd7b224b8b2d2c3dfad780c95c))
* **utils:** add type hints and ADR references to logger modules ([4e6e9fd](https://github.com/terrylica/data-source-manager/commit/4e6e9fd2fdedab7d8009fd06871b894d777f4baf))
* **utils:** add type hints and ADR references to logger modules ([98e6921](https://github.com/terrylica/data-source-manager/commit/98e69219c4be0ebc1284e1410bcea32a8616f834))
* **utils:** add type hints to timeout_logger and session_utils ([3453692](https://github.com/terrylica/data-source-manager/commit/345369202e9533cc85a4f082fe6afeed2079fb39))
* **utils:** replace broad exceptions with specific ParserError types ([ef5eb0c](https://github.com/terrylica/data-source-manager/commit/ef5eb0cdd2567172616c1b218c9db9aa8431e3fd))
* **utils:** replace broad exceptions with specific types ([e2fc13b](https://github.com/terrylica/data-source-manager/commit/e2fc13b122cf5c8e461fec3ca12622bd2fb0ede2))
* **utils:** replace star imports with explicit imports and fix pandas issues ([8e7b25e](https://github.com/terrylica/data-source-manager/commit/8e7b25e3c611f0f72ab285d4d209f1f3e2afbd73))
* **vision-data-client:** replace broad exceptions with specific types ([0b57825](https://github.com/terrylica/data-source-manager/commit/0b57825f826dc27f1c770b37b89e25adc0f58193))


### Performance Improvements

* **checksum:** pre-compile SHA256 regex patterns ([05b7eb5](https://github.com/terrylica/data-source-manager/commit/05b7eb51c0329375b0034ce27405793c055abfed))
* **regex:** pre-compile interval and SHA256 patterns for performance ([d743a7a](https://github.com/terrylica/data-source-manager/commit/d743a7a1f54a868d3648d0ed47d377dd6af4270d))
* **utils:** optimize iteration patterns for better performance ([a19967b](https://github.com/terrylica/data-source-manager/commit/a19967b43387fe3b485551c711bbe667adea77e7))
* **utils:** use list comprehension and extend for efficiency (PERF401/PERF402) ([066102a](https://github.com/terrylica/data-source-manager/commit/066102a62f9cd73f7676315a8214ae44d5901ea4))

# 1.0.0 (2026-01-31)


### Bug Fixes

* address pandas frequency string deprecation and test regressions EL-1009 ([03f6ee7](https://github.com/terrylica/data-source-manager/commit/03f6ee71a977c53a79d85f91928d31ce2367ca44))
* **api-utils:** replace blind exceptions with specific types ([1561826](https://github.com/terrylica/data-source-manager/commit/15618269bbb06629d25f4f09f9bd10d7934ce36a))
* **api-validator:** replace blind exceptions with specific types ([a759ebc](https://github.com/terrylica/data-source-manager/commit/a759ebc5ef40e31c627349f2dc5306a4dbbd9425))
* **arrow-cache:** replace blind exception with specific types ([8742372](https://github.com/terrylica/data-source-manager/commit/874237274bbe0934ade176c225f7df5fdc8d9deb))
* **async:** ensure proper cleanup of AsyncCurl timeout tasks EL-1009 ([be20c48](https://github.com/terrylica/data-source-manager/commit/be20c48727ad9f30f4cd5f5942022efa1c35fe97))
* **autoflake:** unused import cleanup ([9b5e053](https://github.com/terrylica/data-source-manager/commit/9b5e053b35871ae0226d3b5b60003308184a66ce))
* **cache-utils:** replace blind exceptions with specific types ([70c6662](https://github.com/terrylica/data-source-manager/commit/70c666239fd2138092657ff17911b662a266f55e))
* **cache:** Correct cache path structure to align with API and docs EL-1009 ([f01458e](https://github.com/terrylica/data-source-manager/commit/f01458e84f65de15114a60259997f16ae6a4b78f))
* **cache:** implement proper cache statistics aggregation across manager instances EL-1009 ([4976071](https://github.com/terrylica/data-source-manager/commit/49760719ed82e425e84b60e3d7226cc8ae957ded))
* **cache:** replace blind exceptions with specific types ([7042434](https://github.com/terrylica/data-source-manager/commit/7042434ddc353b343456ae9c604d0617df60c8d7))
* **cache:** replace day-based gap detection with proper intraday gap detection ([57216c2](https://github.com/terrylica/data-source-manager/commit/57216c2b707cb2244a9d185ab41a7fefdfe7e639))
* **core/vision_data_client:** Correct API boundary validation and data handling EL-1009 ([30f344b](https://github.com/terrylica/data-source-manager/commit/30f344b1d280c32a7d53d660bc423df3abdfa99a))
* **core:** add filelock and improve enum comparisons for parallel testing EL-1009 ([4049f6d](https://github.com/terrylica/data-source-manager/commit/4049f6d5918fd0d7889e690d1bab3593c8d24165))
* **core:** correct protocol acronym from FCP to PCP-PM in docs and code EL-1009 ([09d4ec0](https://github.com/terrylica/data-source-manager/commit/09d4ec0c6b02a197b3f287a8e6f914553df18918))
* **core:** improve error handling in HTTP client and cache validation EL-1009 ([d013681](https://github.com/terrylica/data-source-manager/commit/d013681fd1874f4c4c2354edde2d04f273cd7028))
* **core:** resolve curl_cffi hanging issues with comprehensive cleanup EL-1009 ([fafe446](https://github.com/terrylica/data-source-manager/commit/fafe44693570fffa74d2acbe5520e23a7599641b))
* **core:** use leading underscore for unused parameters EL-1009 ([5e9aa87](https://github.com/terrylica/data-source-manager/commit/5e9aa878ea31c65117398724da8790a3d0485cdc))
* correct local variable declarations in setup-git-ssh.sh EL-1009 ([21cc6e8](https://github.com/terrylica/data-source-manager/commit/21cc6e840e98afaa68995fa19a74b85c9e5a825b))
* **data_source:** resolve open_time ambiguity and enforce consistent sorting EL-1009 ([388bcff](https://github.com/terrylica/data-source-manager/commit/388bcff5d3440b87597f73f2edebd371edf7b76a))
* **dataframe_output:** remove index from CSV output to improve file readability EL-1009 ([874ff1a](https://github.com/terrylica/data-source-manager/commit/874ff1a65fa851abe3e9070503622e4ab07922c3))
* **dataframe-utils:** replace blind exceptions with specific types ([d461815](https://github.com/terrylica/data-source-manager/commit/d461815d96e257e65f45d2fb8ebeb1a27ebc58e6))
* **demo-utils,logger:** replace all remaining blind exceptions ([e76ba9f](https://github.com/terrylica/data-source-manager/commit/e76ba9f0b7c28ad6cecc18cedb47669c5440d21c))
* **display-utils:** replace blind exceptions with specific types ([db6d287](https://github.com/terrylica/data-source-manager/commit/db6d287257dca69b9e85c81e2093f9d60d6819f4))
* **docs:** correct license reference in README ([65444cd](https://github.com/terrylica/data-source-manager/commit/65444cdd1e56596bc438a2fff783a9ac16d6fd86))
* **docs:** correct markdown table formatting in rate limit README EL-1009 ([78dde54](https://github.com/terrylica/data-source-manager/commit/78dde54c40cb34a0d67930bde1f60c1c1f53b675))
* **docs:** repair broken internal links and add link checker task ([ed3626b](https://github.com/terrylica/data-source-manager/commit/ed3626b68edee448f327a3487542a5e6c95fcc77))
* **dsm_cache_utils:** ensure day boundaries are timezone-aware EL-1009 ([206846f](https://github.com/terrylica/data-source-manager/commit/206846f84624060e74e25d742589b1663798c0b1))
* **dsm-lib:** replace blind exception with specific types ([c8c826f](https://github.com/terrylica/data-source-manager/commit/c8c826f987fc0010e9b307dfe0d20013330263cf))
* **dsm-utilities:** replace blind exceptions with specific types ([6c5348e](https://github.com/terrylica/data-source-manager/commit/6c5348e120c8937af290b16186a0c9cb823ca081))
* **dsm:** Correct auto_reindex=False behavior to prevent NaN values EL-1009 ([5396485](https://github.com/terrylica/data-source-manager/commit/5396485806f1e59e2d267b348a7a91bdc8318064))
* **dsm:** Correct auto_reindex=False behavior to prevent NaN values EL-1009 ([fa88cdf](https://github.com/terrylica/data-source-manager/commit/fa88cdf77839a92a9e244b0447d3440d66a12e6e))
* **dsm:** optimize DataFrame sorting method in cache utils EL-1009 ([cbd0b79](https://github.com/terrylica/data-source-manager/commit/cbd0b791dd0446745604cd24ccbb3771be6e69be))
* **dsm:** Prevent artificial NaN padding via auto_reindex parameter EL-1009 ([fbaf155](https://github.com/terrylica/data-source-manager/commit/fbaf155104625559620d2400f00557f16c2fa6f1))
* **dsm:** replace blind exceptions with specific types ([4e656cb](https://github.com/terrylica/data-source-manager/commit/4e656cb995fc865faee12bd810c1d03d0ca7747e))
* enforce debug code hygiene and iterative testing practices EL-1009 ([de09115](https://github.com/terrylica/data-source-manager/commit/de09115a2becc45ffff45a5dcf164869734db0b4))
* **for_demo:** support log-level shorthand, fix CM symbol and enforce source logging EL-1009 ([fe7ef3a](https://github.com/terrylica/data-source-manager/commit/fe7ef3a0c31f1aca8d617558447c97f16a9108cb))
* **funding-rate:** replace blind exceptions with specific types ([153a1be](https://github.com/terrylica/data-source-manager/commit/153a1be6c81b199985944f03ad1963086abf5097))
* **gap-detection:** debugged gap issues to align with focus_demo.mdc for LSP EL-1009 ([76039a6](https://github.com/terrylica/data-source-manager/commit/76039a68f00e4643bbb8bd5631ad6c1d923aa61a))
* **gap-detector:** replace blind exceptions with specific types ([489813e](https://github.com/terrylica/data-source-manager/commit/489813ee8c7b3b8932542f0e244a3f5b880de850))
* **imports:** complete internal import migration to absolute paths ([096a85b](https://github.com/terrylica/data-source-manager/commit/096a85bd56db5db3093d4eece6c80b27bc909cb3))
* **lint:** auto-fix ruff errors in source code ([d1916d5](https://github.com/terrylica/data-source-manager/commit/d1916d5cd07a425b0afe699559e30029692df28d))
* **linter:** csv ([2a5c883](https://github.com/terrylica/data-source-manager/commit/2a5c883d86be1e64ec2626efcaf01fea1646c694))
* **logger:** improve rich logging configuration and markup handling EL-1009 ([46eabff](https://github.com/terrylica/data-source-manager/commit/46eabffcdac84d186d707d21fd018a6e3010e0e1))
* **logger:** Remove unused rich_tracebacks parameter from get_logger calls EL-1009 ([d444bfc](https://github.com/terrylica/data-source-manager/commit/d444bfcd4b25f149fa2c4414dccbe6d08009662a))
* **logger:** strip rich markup from file logs to improve readability EL-1009 ([b923e61](https://github.com/terrylica/data-source-manager/commit/b923e61b7b0deecd1cb9a48e61d4f5a4ccfbc629))
* **network:** improve error handling and timestamp parsing in network_utils.py EL-1009 ([dba9075](https://github.com/terrylica/data-source-manager/commit/dba9075408ef70a8fe03d5613e680965d5219682))
* **network:** replace blind exceptions with specific types ([2277e0b](https://github.com/terrylica/data-source-manager/commit/2277e0b2c6281a04c51924712db497f9b32b77c2))
* **network:** standardize kline column names using consistent configs EL-1009 ([a63f025](https://github.com/terrylica/data-source-manager/commit/a63f025056064421f983a872eeee6ab3d0b56f75))
* **okx-tests:** Correct status for insufficient reference data EL-1009 ([d7b5cbf](https://github.com/terrylica/data-source-manager/commit/d7b5cbfeb8241423bd93841831467e2c2321c935))
* **okx:** address okx test failures and add fixtures EL-1009 ([c59dbf3](https://github.com/terrylica/data-source-manager/commit/c59dbf37c7850645baafa61025819fbf9c99553e))
* optimize vision data client performance and fix rest client imports EL-1009 ([fc83b33](https://github.com/terrylica/data-source-manager/commit/fc83b33fd454dd4e0ba3ac19c3d448e53010b459))
* Refactor cache validation and path generation for maintainability EL-1009 ([09eece2](https://github.com/terrylica/data-source-manager/commit/09eece2bea03eaa0272631576c5e5e409e8354ce))
* **release:** use curl instead of gh CLI to prevent process storms ([f42c34e](https://github.com/terrylica/data-source-manager/commit/f42c34e9e9b20dcc2e053d8cd55ea57541223e64))
* remove deprecated .cursorignore entries EL-1009 ([438d275](https://github.com/terrylica/data-source-manager/commit/438d2751d931c0a3f49a3d865d0846aa630f89b9))
* remove redundant log file patterns from .gitignore EL-1009 ([34c5c28](https://github.com/terrylica/data-source-manager/commit/34c5c28bbbada554949eec1f4528df738862fa0d))
* replace pivot_table with unstack for Series display in dsm_display_utils - Fixed AttributeError where pivot_table() was called on Series object - Changed to unstack() method which correctly reshapes MultiIndex Series to DataFrame - Resolves timeline display functionality in DSM demo module - Maintains functional correctness for data source breakdown visualization EL-1009 ([22ce335](https://github.com/terrylica/data-source-manager/commit/22ce335c4b1f9ff1b294415a4f84252e4dedffa8))
* resolve Vision API day boundary gap detection and timestamp handling EL-1009 ([973d266](https://github.com/terrylica/data-source-manager/commit/973d2665c6dd6cf1e2320ff3b7ccadef07b12267))
* **rest-client:** replace blind exception with specific types ([afd40b9](https://github.com/terrylica/data-source-manager/commit/afd40b97d0351977e52d9c08abb9cea029a0c128))
* **schema:** remove SchemaStandardizer class in favor of simpler approach EL-1009 ([063cd3a](https://github.com/terrylica/data-source-manager/commit/063cd3abb88e3265560e8287312ae9334024e3ee))
* **scripts:** disable test mode in verify_multi_interval.sh EL-1009 ([031821b](https://github.com/terrylica/data-source-manager/commit/031821b91740b7e100d8a4b19b76ac8ed0c85c7d))
* **scripts:** Increase parallel test workers from 4 to 8 in run_tests.sh EL-1009 ([00135e1](https://github.com/terrylica/data-source-manager/commit/00135e11e6b76a4c25f5043240983a000f29eeba))
* **skills:** use timezone-aware datetime in dsm-usage examples ([af1750d](https://github.com/terrylica/data-source-manager/commit/af1750d4d5f42558b743ddfafb3598bab814251e))
* **ssh-key-management:** enforce single active SSH key and improve verification EL-1009 ([aa784b9](https://github.com/terrylica/data-source-manager/commit/aa784b9e427a22cabfea2a4ddcabf1e75db48a03))
* standardize column names and fix network test regressions EL-1009 ([e482795](https://github.com/terrylica/data-source-manager/commit/e4827959e968c796b88797b705da57253765e3e2))
* standardize timeout values using DEFAULT_HTTP_TIMEOUT_SECONDS constant EL-1009 ([0909331](https://github.com/terrylica/data-source-manager/commit/0909331b340e9380902429f38dbfc97062ff8ec7))
* **structure:** remove legacy dirs, fix exports, add timeout ([f564b11](https://github.com/terrylica/data-source-manager/commit/f564b111445fa3c81aea7e84f4cbd251bafa8927))
* **test,logging:** Resolve regression errors by addressing ResourceWarning and updating logger usage EL-1009 ([37a9d93](https://github.com/terrylica/data-source-manager/commit/37a9d93f16466dfc56864265e2411c9b53000e27))
* **test:** extend clear screen functionality to non-interactive mode EL-1009 ([0166b49](https://github.com/terrylica/data-source-manager/commit/0166b4977b8e449003b77734e4691ab73cc06f12))
* **testing:** eliminate duplicate logging in pytest output EL-1009 ([9aebb01](https://github.com/terrylica/data-source-manager/commit/9aebb0193757d2d04850d5af62c2143d5e89de20))
* **testing:** prevent duplicate logging in test output EL-1009 ([8adb32a](https://github.com/terrylica/data-source-manager/commit/8adb32a024028aa5f4247175f7b02b83acd6db97))
* **tests:** Correctly calculate expected records in time boundary test EL-1009 ([65e805e](https://github.com/terrylica/data-source-manager/commit/65e805ef11a0266c2265a7deccd685e87565a34d))
* **tests:** replace pytest.skip with proper error handling EL-1009 ([823b5c2](https://github.com/terrylica/data-source-manager/commit/823b5c2d3926eaba032b70be0523734e38cc6e45))
* **tests:** update test suite for src-layout compatibility ([3e3c782](https://github.com/terrylica/data-source-manager/commit/3e3c78280f9eab1875361af4fe66721930130bdf))
* **tests:** update time boundary handling to properly handle exclusive end times EL-1009 ([9cf6e33](https://github.com/terrylica/data-source-manager/commit/9cf6e33586f5b9d18f2061da48847639bafe383f))
* **time_alignment:** Correctly handle microsecond precision and inclusive time boundaries EL-1009 ([62125c2](https://github.com/terrylica/data-source-manager/commit/62125c2b99dd6d525af9f4f8c7cbab529e3f745c))
* **time-boundaries:** correct vision API data filter for exclusive end time EL-1009 ([b40444d](https://github.com/terrylica/data-source-manager/commit/b40444d7c990ef886db9f2b0abf08b9d5971140a))
* **time-range-validation:** Relax time boundary validation to include start boundary data EL-1009 ([e0d152b](https://github.com/terrylica/data-source-manager/commit/e0d152beab79424b91cad005e5edf50acedbe12b))
* **time-utils:** update dataframe filtering to use inclusive end boundary EL-1009 ([18cccd8](https://github.com/terrylica/data-source-manager/commit/18cccd8f4e1ef1a321fb6be1ab02919125c59b79))
* **timeout:** implement robust curl_cffi cleanup to prevent false timeouts EL-1009 ([2765c0f](https://github.com/terrylica/data-source-manager/commit/2765c0f0277600bf32a91301b875b00952a3ca70))
* **time:** resolve critical time alignment bug causing hourly interval failures EL-1009 ([4ce39e5](https://github.com/terrylica/data-source-manager/commit/4ce39e50b8a5e961e167a45ba20908a6932f641d))
* **types:** remove type ignore annotations in vision_data_client EL-1009 ([5a53c0c](https://github.com/terrylica/data-source-manager/commit/5a53c0cb593089930bf8101fac7d0c5e0c50ab9c))
* Use timezone-aware datetime for cache path mapping EL-1009 ([cb3be46](https://github.com/terrylica/data-source-manager/commit/cb3be461b7fc582a91b0d7ca4c3a6132887770e2))
* **utils:** replace blind exceptions with specific types ([e5b0500](https://github.com/terrylica/data-source-manager/commit/e5b05000969250cd1599d12a43b814bef5f88440))
* **utils:** replace blind exceptions with specific types ([01e9197](https://github.com/terrylica/data-source-manager/commit/01e91970b213651aa1e032313b7d32157f074735))
* **validation:** improve date type handling in data source manager EL-1009 ([50e2dbd](https://github.com/terrylica/data-source-manager/commit/50e2dbd4b379f610d715ee534b69d711922dd36f))
* **vision_api:** correct CSV header misalignment and data gap issues EL-1009 ([a6e7814](https://github.com/terrylica/data-source-manager/commit/a6e78145c7f8cd40708c690b1c4fd6908efaeab7))
* **vision-checksum:** replace blind exceptions with specific types ([3d9e2ff](https://github.com/terrylica/data-source-manager/commit/3d9e2ff82e228a281a70de0d59e1880ce7bb441f))
* **vision-client:** enhance interval validation and URL construction EL-1009 ([6edbc29](https://github.com/terrylica/data-source-manager/commit/6edbc29c33c6703030675ff63bc1eae0904ccfc8))
* **vision-path:** replace blind exceptions with specific types ([e49f940](https://github.com/terrylica/data-source-manager/commit/e49f9405b375788176c1c783fd9c4ded4e4ffd48))
* **vision-utils,df-types:** replace blind exceptions with specific types ([f65fbc9](https://github.com/terrylica/data-source-manager/commit/f65fbc9499be8142bee79ff499e65f410cfe8ce1))
* **vision:** enhance checksum verification and error handling for VISION API EL-1009 ([5f82634](https://github.com/terrylica/data-source-manager/commit/5f8263486495084d5d9b2a4a0fe5a7b0db1464e8))
* **vision:** replace blind exceptions with specific types ([00d6115](https://github.com/terrylica/data-source-manager/commit/00d611504e59dec032a61bd43516c3db1a2f5129))
* **warnings:** eliminate false positives for historical data EL-1009 ([6960655](https://github.com/terrylica/data-source-manager/commit/69606554a2eee7a469113eb863e07084d34ec892))
* **warnings:** improve historical data coverage messaging accuracy EL-1009 ([6889597](https://github.com/terrylica/data-source-manager/commit/68895975b94101fc15dff4091f0ee1f9f9ae9ace))


### Features

* add comprehensive market type validation and metadata enrichment ([b4ca1a4](https://github.com/terrylica/data-source-manager/commit/b4ca1a47fcc3370c74456e0bf6ee7d7440bacdfe))
* add debug_chunk.mdc rule for debug code organization EL-1009 ([0e4c4c4](https://github.com/terrylica/data-source-manager/commit/0e4c4c40cd34a6d5a1680276526c1cfd93106236))
* Add log file path output to Bybit downloader EL-1009 ([a61d36c](https://github.com/terrylica/data-source-manager/commit/a61d36ca4b5ad630b33f59cc49777cefe6e53917))
* Add log file path output to Bybit downloader EL-1009 ([8e162f1](https://github.com/terrylica/data-source-manager/commit/8e162f1ca07b467c849a95d74432a44755b1a15b))
* add OKX data provider support EL-1009 ([725e733](https://github.com/terrylica/data-source-manager/commit/725e733c78d432bf71b55b0f76c1ea3fc2785d43))
* add package initialization files EL-1009 ([fee7835](https://github.com/terrylica/data-source-manager/commit/fee7835a0bef6835ff2edead5c38322fcc123f89))
* Add Polars and NumPy for data processing EL-1009 ([928f8ef](https://github.com/terrylica/data-source-manager/commit/928f8ef4d7246b87fe1026a5ba6206ae84e69617))
* Add ruff configuration and update devcontainer EL-1009 ([f605b7c](https://github.com/terrylica/data-source-manager/commit/f605b7c3dc0a25057e7cdc46625d2ed84e90ad02))
* add simple data retrieval example script EL-1009 ([ef84326](https://github.com/terrylica/data-source-manager/commit/ef84326e2f48b21315ae3dd0a514f03de7c3be70))
* **agents:** add Claude Code subagents and verification guidance ([3c0a472](https://github.com/terrylica/data-source-manager/commit/3c0a472eef6cf7bb90f3e0e81febfc5de5167bd4))
* **agents:** add hooks frontmatter to test-writer agent ([d8c389e](https://github.com/terrylica/data-source-manager/commit/d8c389e60baaa774e33d8d8acd531be6bac91104))
* **agents:** add silent-failure-hunter and fcp-debugger agents ([86c0685](https://github.com/terrylica/data-source-manager/commit/86c0685e24ccfd55ec20495c03a970043019cc6e))
* **agents:** add test-writer subagent for test development ([18c1adf](https://github.com/terrylica/data-source-manager/commit/18c1adfb1e0740a99e97a517b3da0ea66e93e2e5))
* **analysis:** enhance data source and coverage analysis with terminal-based tools EL-1009 ([879534d](https://github.com/terrylica/data-source-manager/commit/879534ddffbe45e5a40d19a4fab80315f5cf5675))
* **api_boundary_validator:** Implement ApiBoundaryValidator and move test file EL-1009 ([0087406](https://github.com/terrylica/data-source-manager/commit/0087406c478998176be5754e37c48ffa81f91a56))
* **api_boundary_validator:** Implement ApiBoundaryValidator and tests (Phase 1 revamp_time_alignment) EL-1009 ([59e112e](https://github.com/terrylica/data-source-manager/commit/59e112ee21127533cacaf28f59dba076ca702d67))
* **api:** enforce LSP principles in timestamp boundary handling EL-1009 ([c2cc8e4](https://github.com/terrylica/data-source-manager/commit/c2cc8e40bf9ef92c70ffe6d8994b6a5e292b8402))
* **arrow_cache:** add test scripts for market constraints and timestamp detection EL-1009 ([b773389](https://github.com/terrylica/data-source-manager/commit/b77338928dfa345c0ea01e6283e19a0d2eb4142e))
* **arrow-cache:** implement market parameters and incremental updates EL-1009 ([2d5db56](https://github.com/terrylica/data-source-manager/commit/2d5db565f2efb956b1d74b2c5f8254f77779e441))
* **aws-s3-fetch:** optimize performance and add caching for Binance data fetch EL-1009 ([e6a94d4](https://github.com/terrylica/data-source-manager/commit/e6a94d494c57db583ea604900d4286baca88a0dc))
* **benchmark:** add SHA-256 checksum validation to fsspec benchmark EL-1009 ([a239162](https://github.com/terrylica/data-source-manager/commit/a23916256ca602da313923af829036416dabf86f))
* **binance:** Ensure date is pendulum.DateTime in VisionPathMapper EL-1009 ([c13478b](https://github.com/terrylica/data-source-manager/commit/c13478b7ad0279948b2de833a018e5ecddfd58ea))
* **build:** Bump version and update dependencies EL-1009 ([028783e](https://github.com/terrylica/data-source-manager/commit/028783ef3cd9b35ae0180b263f5fe0375485a082))
* **build:** Bump version to 0.1.45 EL-1009 ([33c8bd5](https://github.com/terrylica/data-source-manager/commit/33c8bd5dbe5ef5871f2e9015901ab1a480d46542))
* **build:** Bump version to 0.1.53 EL-1009 ([e956f91](https://github.com/terrylica/data-source-manager/commit/e956f91b0423e5a39c98ee0a7e0a22c9a06bb391))
* **build:** Bump version to 0.1.54 EL-1009 ([bca652f](https://github.com/terrylica/data-source-manager/commit/bca652f8adafb26dfcea0984322a185cac30c1eb))
* **build:** Bump version to 0.1.70 EL-1009 ([5bd398e](https://github.com/terrylica/data-source-manager/commit/5bd398ecefce3d452920e2853575dd0ea9966bf5))
* **bybit/download-script:** Simplify output filename generation (EL-1009) ([0a450bb](https://github.com/terrylica/data-source-manager/commit/0a450bb0719fdfe33f37ef8a0d5b02e3298be69f))
* **bybit:** Add documentation and test script for kline data non-overlap EL-1009 ([6d409a4](https://github.com/terrylica/data-source-manager/commit/6d409a46ff863328b259950e435edbb930367617))
* **bybit:** Add script for downloading historical kline data EL-1009 ([894375b](https://github.com/terrylica/data-source-manager/commit/894375be0cd2f4c422e30a4f10c54039b78afc4e))
* **bybit:** Enhance klines download with symbol and category validation EL-1009 ([6d2ad83](https://github.com/terrylica/data-source-manager/commit/6d2ad8316d349e1f837edfb4f48d97b1c1a7b4f7))
* **bybit:** Integrate loguru for enhanced logging in download script EL-1009 ([a9e6a14](https://github.com/terrylica/data-source-manager/commit/a9e6a14849fa375d0436512eea04d7501848c052))
* **cache,test:** Implement API boundary validation and correct cache path structure EL-1009 ([ac03c78](https://github.com/terrylica/data-source-manager/commit/ac03c7859d211b7d63861dbd3f1fe1c96e921abd))
* **cache:** implement cache key management demo and FCP refactoring EL-1009 ([4acb0d3](https://github.com/terrylica/data-source-manager/commit/4acb0d3ee4948c522a4c11341269064d24f33c9a))
* **cache:** implement comprehensive cache key management system EL-1009 ([9e14c40](https://github.com/terrylica/data-source-manager/commit/9e14c403a5a7393bce30f9385fddabe3cad5dff0))
* **cache:** Implement new cache path structure with exchange, market type, data nature, and packaging frequency parameters EL-1009 ([068228b](https://github.com/terrylica/data-source-manager/commit/068228b648752a698e9ce3ad7cf977f8409f60e8))
* **cache:** refactor cache key generation and path structure to include market type EL-1009 ([e159f56](https://github.com/terrylica/data-source-manager/commit/e159f56419e55ff9cde56216aefdd2e305d1abd9))
* **cache:** restructure cache directory hierarchy for futures markets EL-1009 ([bb231f8](https://github.com/terrylica/data-source-manager/commit/bb231f8f67d3379bf940676d154768116d49eb4a))
* **cache:** update cache path structure for enhanced organization and flexibility EL-1009 ([c7f2dad](https://github.com/terrylica/data-source-manager/commit/c7f2dade1102d5b790d584bd451e533e9f46f034))
* Change default logging level to ERROR for quieter operation EL-1009 ([556bfea](https://github.com/terrylica/data-source-manager/commit/556bfea0c4f761729e813c9a4c36993c45ff2a2b))
* **checksum:** enhance checksum verification and add test scripts EL-1009 ([890c6de](https://github.com/terrylica/data-source-manager/commit/890c6deeff1bcc809e0e6e597bb0057845bcf87c))
* **claude:** add argument-hint, allowed-tools to commands and adr to rules ([b2b49f0](https://github.com/terrylica/data-source-manager/commit/b2b49f0676abfbc4ea1751e5a99eb6c20853b7f5))
* **claude:** add context optimization best practices from online sources ([0b1c0b2](https://github.com/terrylica/data-source-manager/commit/0b1c0b2382d7fa6bae843b3015e62db8b83e9d49))
* **claude:** add monorepo-style domain CLAUDE.md files ([a35fe0e](https://github.com/terrylica/data-source-manager/commit/a35fe0eb1fce363734fd8755e4e52d41f0dbb9c3))
* **claude:** add path-specific rules and enhance agents ([e00adf2](https://github.com/terrylica/data-source-manager/commit/e00adf2c51375e5875021d6fbc98ca8bbe54adf5))
* **claude:** add settings.md and infrastructure validation ([659a44c](https://github.com/terrylica/data-source-manager/commit/659a44cb11e649b1b24069bf44aabc5e98b2e673))
* **claude:** add src/CLAUDE.md for monorepo-style lazy loading ([62a83a7](https://github.com/terrylica/data-source-manager/commit/62a83a7758d22c6aeebf52b506034d210e48472e))
* **claude:** add Stop hook and agent color field ([a097b27](https://github.com/terrylica/data-source-manager/commit/a097b27f3e87d76080e70352bb39675811451f8e))
* **claude:** apply cc-skills patterns to infrastructure ([8e9e865](https://github.com/terrylica/data-source-manager/commit/8e9e865c27f14c54da60f4db4cc467b7c0021de7))
* **cleanup:** centralize async resource cleanup pattern for Python 3.13+ EL-1009 ([4bdcf38](https://github.com/terrylica/data-source-manager/commit/4bdcf386de1b61b48a2ab2ea89ac8914ca0749d1))
* **clear-cache-script:** Add --create option and improve logging EL-1009 ([d4d215c](https://github.com/terrylica/data-source-manager/commit/d4d215c3da88b737b7e12d0f84be598d055b3269))
* **cli:** enhance FCP-PM demo with shorthand flags and help improvements EL-1009 ([e0fbadd](https://github.com/terrylica/data-source-manager/commit/e0fbadd31243a108a11d3adae11016cd45b2f6d7))
* **commands:** add Claude Code custom commands for data operations ([778bc54](https://github.com/terrylica/data-source-manager/commit/778bc5432a9164b715048977f09e53f776d377e9))
* **commands:** add debug-fcp and quick-test commands ([2d82977](https://github.com/terrylica/data-source-manager/commit/2d82977ffe10f9c4c5b1ff0f6d04a03d830482da))
* **commands:** add disable-model-invocation for side-effect commands ([5f75a45](https://github.com/terrylica/data-source-manager/commit/5f75a4546dc901caeb8ee561d9cd862a5ef0b101))
* **commands:** add DSM code review command ([15473e6](https://github.com/terrylica/data-source-manager/commit/15473e63a325ea6bbf39ab8e5e175bca4c1e7ce5))
* **commands:** add feature-dev workflow for DSM development ([3f6848f](https://github.com/terrylica/data-source-manager/commit/3f6848f03d374b0dc06bafa568624561be7b32c5))
* **core, demo:** centralize and enhance date range calculation logic EL-1009 ([9e2b0ef](https://github.com/terrylica/data-source-manager/commit/9e2b0ef3232c1bebde6dce9a814972bdd559d28a))
* **core, utils:** Centralize data validation and enhance timezone handling EL-1009 ([92000fc](https://github.com/terrylica/data-source-manager/commit/92000fc34b7737ea25d5e7e673434ddbe3486ea0))
* **core/sync/rest_data_client:** remove unused dead code and redundant declarations EL-1009 ([0e36d3e](https://github.com/terrylica/data-source-manager/commit/0e36d3e6573d1402a9c8992bcc8a83f7298a793f))
* **core/sync:** centralize Vision API freshness guard and skip retries for recent dates EL-1009 ([e360671](https://github.com/terrylica/data-source-manager/commit/e360671fa030b5626cc12bf7b778f954e43b7191))
* **core/sync:** enhance FCP-PM mechanism with robust merge operations EL-1009 ([930f23c](https://github.com/terrylica/data-source-manager/commit/930f23cf0b36bfeeff08889be7d154127b507047))
* **core/sync:** upgrade FCP to FCP-PM with iterative merge strategy EL-1009 ([4d5953d](https://github.com/terrylica/data-source-manager/commit/4d5953d41ec6300c7be800a385b015be7026d443))
* **core:** add multi-provider support and funding rate API EL-1009 ([ed9a94e](https://github.com/terrylica/data-source-manager/commit/ed9a94e8878dbda47f3070afbd4f97a34ca86f7e))
* **core:** implement download-first approach and automatic API fallback EL-1009 ([d5c08f7](https://github.com/terrylica/data-source-manager/commit/d5c08f7bb3cf3b21ea3915af8ab5f8e88786fe2a))
* **core:** implement lazy initialization for faster imports EL-1009 ([df756f3](https://github.com/terrylica/data-source-manager/commit/df756f352b8fbde433ed25fc893cd80ecf4295a5))
* **data_source_manager:** enhance FCP-PM mechanism with iterative merge and gap detection EL-1009 ([0358b7b](https://github.com/terrylica/data-source-manager/commit/0358b7b4bf4b9ee2246da7af83245481e220c395))
* **data_source_manager:** refactor gap handling and REST fallback logic EL-1009 ([12c3592](https://github.com/terrylica/data-source-manager/commit/12c359257a5e2eb30cee743d5e010dd4131566fa))
* **data_source_manager:** remove unused parameters and debug code EL-1009 ([d5f39d9](https://github.com/terrylica/data-source-manager/commit/d5f39d934681458aeb3e19b1984ddab6643c1d35))
* **data-availability:** add recency check for Binance symbols EL-1009 ([bfa89e4](https://github.com/terrylica/data-source-manager/commit/bfa89e4d311164537b1eb5aa8d8f37632af2c8b1))
* **data-source:** enhance FCP-PM error handling and failover behavior EL-1009 ([01e7b67](https://github.com/terrylica/data-source-manager/commit/01e7b67ce0f68870528f447fe17731f93a559c99))
* **data-source:** enhance FCP-PM with conditional Vision API failure tolerance EL-1009 ([ccb2af1](https://github.com/terrylica/data-source-manager/commit/ccb2af190cabef14235d521142549e403e4ca947))
* **data-source:** implement FCP-PM mechanism with daily pack caching EL-1009 ([e650a45](https://github.com/terrylica/data-source-manager/commit/e650a45cd2bd68e7cc900226c354e03449ffff23))
* **data:** standardize column names for kline data EL-1009 ([864331d](https://github.com/terrylica/data-source-manager/commit/864331dfd18f21cd961a31ad307a9199e5f67d52))
* **data:** Update earliest dates and intervals for Binance symbols EL-1009 ([1706c32](https://github.com/terrylica/data-source-manager/commit/1706c32eac61e5505a2059c4437a82deb72a7743))
* **dead-code-reporting:** Replace shell script with Python tool EL-1009 ([6fc11c2](https://github.com/terrylica/data-source-manager/commit/6fc11c204a5dde9643a59d023b64f3bf01afd52d))
* **debug:** Implement timezone-aware FAIL-FAST debugging utilities ([febe4eb](https://github.com/terrylica/data-source-manager/commit/febe4eb9850f4e13981352e1a927afa63c425260))
* **debug:** Implement timezone-aware FAIL-FAST debugging utilities ([9a8384d](https://github.com/terrylica/data-source-manager/commit/9a8384dfe437bee0177e0c6870691dc375a3194e))
* **demo:** enhance FCP demo with auto-documentation and linting support EL-1009 ([592a678](https://github.com/terrylica/data-source-manager/commit/592a678bebafd30db8374d80eecb3b488b80bbf8))
* **demo:** make FCP merging default behavior and simplify CLI interface EL-1009 ([3f18a60](https://github.com/terrylica/data-source-manager/commit/3f18a6079d8dc9bfdebc8f38700e82b18ccb1e20))
* **demo:** remove Time Range Options from DSM demo docs EL-1009 ([28bd839](https://github.com/terrylica/data-source-manager/commit/28bd839daa9e43d076ed1c419c83d90b7af9eed2))
* **deps:** add attrs dependency EL-1009 ([c8334c5](https://github.com/terrylica/data-source-manager/commit/c8334c5f1a448f7e39919589a4d666741f6095b6))
* **deps:** add pendulum for datetime handling EL-1009 ([47f9dd7](https://github.com/terrylica/data-source-manager/commit/47f9dd752493d4e3d97df09dcffbccee1bcf9c85))
* **deps:** add pyright for static type checking EL-1009 ([4aab65f](https://github.com/terrylica/data-source-manager/commit/4aab65f7fd7f198cfcb171c1693f333a05b46fe2))
* **dev-env:** add Ruff, Pylint, and GitPython dev dependencies and devcontainer extension EL-1009 ([f3a221d](https://github.com/terrylica/data-source-manager/commit/f3a221d6ec9444099e1ccdef1ac5d991b5a401fe))
* **dev-scripts:** Add Ruff linter report script EL-1009 ([7ae2f10](https://github.com/terrylica/data-source-manager/commit/7ae2f10da94d0504f1a4fb4a9b1d2c20e4d18bf1))
* **devcontainer:** add GitHub CLI integration for seamless repository operations ([4632a40](https://github.com/terrylica/data-source-manager/commit/4632a405cc09ac9e1d11f0b60ab97f5fbc6c7ac9))
* **devcontainer:** add markdown editor extension EL-1009 ([321524a](https://github.com/terrylica/data-source-manager/commit/321524acbf4b6d6f7305663da94dc4d3713714b3))
* **devcontainer:** enhance VS Code extensions setup EL-1009 ([6fb89e4](https://github.com/terrylica/data-source-manager/commit/6fb89e4e828b491f410012d3363cebdfa886c369))
* **devcontainer:** Update Docker and devcontainer config EL-1009 ([dee7892](https://github.com/terrylica/data-source-manager/commit/dee7892e51d927dbcbadc415bdc5ec209870f357))
* **docs, core:** Enhance README and expose core components for library usage EL-1009 ([e32b7b1](https://github.com/terrylica/data-source-manager/commit/e32b7b1c580046a880654455c8310378ece1bd83))
* **docs/okx:** Add REST API candlestick data documentation EL-1009 ([864691e](https://github.com/terrylica/data-source-manager/commit/864691eb2b646c4e77c7585aff31d798f28ca894))
* **docs:** Add Bybit REST API kline endpoint documentation EL-1009 ([30c1351](https://github.com/terrylica/data-source-manager/commit/30c13510261bcd7ff8840357464ea29c17b21375))
* **docs:** add data availability script usage and improve CSV handling EL-1009 ([634906d](https://github.com/terrylica/data-source-manager/commit/634906d9bbbb0663c8103833b564dd018abccfef))
* **docs:** add hub-and-spoke documentation structure ([b83835e](https://github.com/terrylica/data-source-manager/commit/b83835e6b072965f332664d34bc9b08489b47b45))
* **docs:** Add script to generate READMEs in markdown directories EL-1009 ([ac37c2b](https://github.com/terrylica/data-source-manager/commit/ac37c2bb8e6f6c1e7a85ce574c2690b70b45678d))
* **dsm_cli_utils:** add DataProviderChoice enum to CLI utils EL-1009 ([d080ee5](https://github.com/terrylica/data-source-manager/commit/d080ee509a0c1cd6a86211198bc0cb376f9ebce3))
* **dsm_demo:** enhance FCP demo display and remove deprecated test flags EL-1009 ([04ab61e](https://github.com/terrylica/data-source-manager/commit/04ab61ebffe405ce6ba952b906950893ae133969))
* **dsm-demo:** extract DSM core logic to library and add showcase EL-1009 ([efad319](https://github.com/terrylica/data-source-manager/commit/efad31917b3ebe51b33f692c9aff6732d7e36c06))
* **dsm-example:** Enhance logging and environment info in DSM example module EL-1009 ([b5a5334](https://github.com/terrylica/data-source-manager/commit/b5a533438eee3ece239a5361f13fa87f57f26125))
* **dsm-logging:** Implement comprehensive logging control EL-1009 ([2926a98](https://github.com/terrylica/data-source-manager/commit/2926a9816dc3e14d0f1df2b6a66ef051d5f2f4f2))
* **dsm:** enhance demo module with display functionality EL-1009 ([fe02c8c](https://github.com/terrylica/data-source-manager/commit/fe02c8c0e001438e4e4690379ed170ee141b8b68))
* **dsm:** use project root for cache directory EL-1009 ([4280507](https://github.com/terrylica/data-source-manager/commit/4280507f3e22cc0a696bf731a6b6d1aa6626d4f1))
* **dx:** add coverage config, CONTRIBUTING.md, and Python version pin ([61f3161](https://github.com/terrylica/data-source-manager/commit/61f316183d40cbeb15d8c9a774cc0c33c53da503))
* enhance async HTTP task handling with event-based cancellation and cleanup EL-1009 ([cfaaba8](https://github.com/terrylica/data-source-manager/commit/cfaaba8ebd2b0c5dae806033cea0cc2aad78c34c))
* enhance asyncio error reporting in pytest with detailed summaries EL-1009 ([414570a](https://github.com/terrylica/data-source-manager/commit/414570a35a91e5dcfb300ba9d962b9252980424b))
* Enhance FCP mechanism logging and refactor CLI options EL-1009 ([f2a7219](https://github.com/terrylica/data-source-manager/commit/f2a72195fdcd5d8e232bce171723b07b4b2f27c6))
* **error-tracking:** design modular error tracking system EL-1009 ([4bd6358](https://github.com/terrylica/data-source-manager/commit/4bd63581cc63a7b90c28b201e4226316953e8652))
* **examples:** rename recommended_data_retrieval.py to data_retrieval_best_practices.py EL-1009 ([171e003](https://github.com/terrylica/data-source-manager/commit/171e003c0eac75e039c45d1fc76aabc249564cd3))
* explicitly set Binance as data provider in DataSourceManager EL-1009 ([3a87375](https://github.com/terrylica/data-source-manager/commit/3a87375584a05319f4453b2958c1e906206d5cfb))
* **fcp_demo:** enhance error handling and date range logic EL-1009 ([a22a931](https://github.com/terrylica/data-source-manager/commit/a22a9319cc7d3b47715138e394391fa42cc697db))
* **fcp_demo:** enhance markdown documentation generation with typer-cli fallback EL-1009 ([45bbf4d](https://github.com/terrylica/data-source-manager/commit/45bbf4d9dc0c187417f7323e6eba8fbe6b37a8ac))
* **fcp_pm:** add comprehensive test harness and CLI flags for FCP-PM mechanism EL-1009 ([8dff764](https://github.com/terrylica/data-source-manager/commit/8dff7649c91f247949dabf6f47bf84917bfb9b64))
* **fcp-pm:** implement cache testing framework and optimize checksum logging EL-1009 ([0f2ed6c](https://github.com/terrylica/data-source-manager/commit/0f2ed6c0c2513aaec2646061f3f8d5fcd39777a4))
* **fcp:** clarify time range priority hierarchy and improve help text EL-1009 ([e26d706](https://github.com/terrylica/data-source-manager/commit/e26d70673b94de30d878af5fba7b59892692e80a))
* **fsspec-dsm-example:** Add debug logging for path mapping EL-1009 ([b9c3518](https://github.com/terrylica/data-source-manager/commit/b9c3518dc88995862f916c7d0359d6144e0df6f7))
* **funding-rate:** add funding rate downloader tools and examples EL-1009 ([c83b9ee](https://github.com/terrylica/data-source-manager/commit/c83b9ee33d2754deb1c22823644867f73d505786))
* **gap_detector:** enforce Interval enum and minimum timespan EL-1009 ([f186c1d](https://github.com/terrylica/data-source-manager/commit/f186c1dca93079ec139323576d4c8cdad1471a95))
* **hooks:** add DataFrame validation and Polars preference checks ([1509399](https://github.com/terrylica/data-source-manager/commit/1509399a93b5c0ad0dd8601b1213d66d59721b4b))
* **hooks:** add description and notes fields to hooks.json ([24551cb](https://github.com/terrylica/data-source-manager/commit/24551cbb64e00895d1eb559111cdad7b6b5c22d3))
* **hooks:** add DSM-specific checks to code guard ([39992d8](https://github.com/terrylica/data-source-manager/commit/39992d8ae95248d95a6c068603a26432c5eec836))
* **hooks:** add DSM-specific code guard hook ([2a6c795](https://github.com/terrylica/data-source-manager/commit/2a6c79505f2a8952922aefba64597a6803e1d265))
* **hooks:** add PreToolUse bash guard for command validation ([a72c1de](https://github.com/terrylica/data-source-manager/commit/a72c1dea94672953e1f3fd17af819068420aa9c8))
* **hooks:** add SessionStart hook for FCP context loading ([8f6dbb0](https://github.com/terrylica/data-source-manager/commit/8f6dbb00bae42f6d592b94e76b5971a8c7a7dd42))
* **hooks:** add UserPromptSubmit skill suggestion hook ([2ffdd87](https://github.com/terrylica/data-source-manager/commit/2ffdd87fdf759ccf9d8680248f6445615aaadd7f))
* implement centralized timeout handling and logging in data clients EL-1009 ([73a701c](https://github.com/terrylica/data-source-manager/commit/73a701c69f7963be60034477db624766b82d9c20))
* Implement comprehensive datetime handling and data completeness checks EL-1009 ([442b10e](https://github.com/terrylica/data-source-manager/commit/442b10ea882026f5b081010a977e68fb1b7b0cf3))
* implement FCP-PM mechanism for data retrieval EL-1009 ([fdce080](https://github.com/terrylica/data-source-manager/commit/fdce080e95ef44f000b417ef52260faf5459f2ee))
* implement multi-symbol processing with improved date detection EL-1009 ([9f9c505](https://github.com/terrylica/data-source-manager/commit/9f9c505fbefe9f8add9cd8237e3d533f69737cf4))
* improve task cancellation handling and reduce warning noise EL-1009 ([e2c71a3](https://github.com/terrylica/data-source-manager/commit/e2c71a3d9399dfc2a5841013f9b4c537c0f99f30))
* **interval:** validate supported intervals early and enforce error handling (EL-1009) ([d68cd19](https://github.com/terrylica/data-source-manager/commit/d68cd19ec29c272e2cdbc49d8ea4bc84856daee6))
* Introduce constants for improved maintainability and readability EL-1009 ([948befb](https://github.com/terrylica/data-source-manager/commit/948befb1b0505077b2f7b4d1047c992caef674a7))
* **logger:** add dedicated error logging functionality EL-1009 ([b0bc8b2](https://github.com/terrylica/data-source-manager/commit/b0bc8b2fffe03407014028b753fabc78ac8c5f2a))
* **logger:** add rich logging integration support EL-1009 ([e81edfb](https://github.com/terrylica/data-source-manager/commit/e81edfbe2d91d5e9608f750856dfb56dbf07cfaf))
* **logger:** enhance logger with error context support for better testing EL-1009 ([4cb3b41](https://github.com/terrylica/data-source-manager/commit/4cb3b41635993868ad938346ad55145e438c1e90))
* **logger:** force color output in container environments EL-1009 ([1f0975b](https://github.com/terrylica/data-source-manager/commit/1f0975b90da6a9cccba8a6fa7fd8b757341fc462))
* **logger:** implement conventional setLevel API as replacement for setup EL-1009 ([d86f547](https://github.com/terrylica/data-source-manager/commit/d86f5478387661be88c29c79bff6a1421f181172))
* **logger:** implement hierarchical logger with proxy pattern EL-1009 ([43974d8](https://github.com/terrylica/data-source-manager/commit/43974d8e51c5d5fdbaf1c67bbea19cff7c31acaa))
* **logger:** Upgrade to StreamHandler with colorlog for pytest caplog compatibility EL-1009 ([0545a9a](https://github.com/terrylica/data-source-manager/commit/0545a9a56968d6e885b339cfc0341c65cc8d7340))
* **logging:** add session-based logging configuration with timestamped files EL-1009 ([7c225b2](https://github.com/terrylica/data-source-manager/commit/7c225b25b6045c6f1a3e013922d3fa6c190759ab))
* **logging:** enhance LoggerProxy with file handler support and method chaining EL-1009 ([7f340ce](https://github.com/terrylica/data-source-manager/commit/7f340ce39a37e662669358317ebd8d2124b225b9))
* **logging:** introduce enhanced logging system with rich formatting and method chaining EL-1009 ([e5783aa](https://github.com/terrylica/data-source-manager/commit/e5783aa3b26ca41787f50f37cbb6eac175817bbe))
* **market-constraints:** add symbol-market validation logic and integrate in demo CLI EL-1009 ([060ed76](https://github.com/terrylica/data-source-manager/commit/060ed769d5c09701179fde40a0c7edb5f394a45e))
* merge comprehensive market type validation and metadata enhancement ([641600f](https://github.com/terrylica/data-source-manager/commit/641600f2d2a1fae5e882b92721b1cf0dcd74d3ec))
* Migrate project packaging to pyproject.toml and update devcontainer setup EL-1009 ([be6620e](https://github.com/terrylica/data-source-manager/commit/be6620e5ea30d687b4618059857d36cb6776ded1))
* migrate to loguru-based logging system EL-1009 ([040bc1b](https://github.com/terrylica/data-source-manager/commit/040bc1b837b248a23a6474af19dc83d69606a858))
* **mise:** add cache management tasks ([392651e](https://github.com/terrylica/data-source-manager/commit/392651ec0363d0c676694449ece9514aab3db7ff))
* **mise:** add FCP diagnostics and quick check tasks ([19d4b48](https://github.com/terrylica/data-source-manager/commit/19d4b48ddccbb8e061cbd9d2a848deceff09839c))
* **mise:** enhance task runner with sources/outputs and help command ([eb56ffb](https://github.com/terrylica/data-source-manager/commit/eb56ffbf9e2069936c8de93c648c63b208dcaa47))
* **network:** Centralize network config using utils/config EL-1009 ([10278f3](https://github.com/terrylica/data-source-manager/commit/10278f3f2f7006034b56e3678aec158a96eb7a74))
* **okx:** Add instrument analysis script and OKX REST API guide EL-1009 ([ead53ae](https://github.com/terrylica/data-source-manager/commit/ead53ae66a87f3ff14f764189217a0d771b96118))
* **okx:** Add OKX API integration guide EL-1009 ([d6c11b3](https://github.com/terrylica/data-source-manager/commit/d6c11b3b0bbe75455cd319917c8b6707abe14bf8))
* **okx:** Add OKX data explorer CLI tool EL-1009 ([aeb681b](https://github.com/terrylica/data-source-manager/commit/aeb681b868190fc2204fa1ce46ab519d2d0e66c7))
* **playground/bybit:** Add comprehensive README for data tools EL-1009 ([5d951fa](https://github.com/terrylica/data-source-manager/commit/5d951fafcaac9d1f970022aba0b844db476cf70c))
* **playground:** add Binance Vision API checksum verification tests and demo EL-1009 ([f99a6bb](https://github.com/terrylica/data-source-manager/commit/f99a6bbc05c92c594db403c914faecbf36d8ce99))
* **playground:** implement fsspec-based path mapper for Binance Vision API EL-1009 ([94e5836](https://github.com/terrylica/data-source-manager/commit/94e5836b741f1667de38498d3451a018026e3c53))
* **rate-limit:** analyze binance api rate limit behavior for optimal data retrieval EL-1009 ([a418696](https://github.com/terrylica/data-source-manager/commit/a418696e4b38cdeca6c0c239fc9702ba98cdf4c8))
* Refactor cache structure and align Vision API to REST API boundaries EL-1009 ([103cea0](https://github.com/terrylica/data-source-manager/commit/103cea039788a60ac6b09b2a824f45ad9b9030d4))
* Refactor path and cache management utilities EL-1009 ([4fbf294](https://github.com/terrylica/data-source-manager/commit/4fbf294e35194bdb4806337c8872c13ec4d00c23))
* **refactor-move:** Add comprehensive import refactoring and validation EL-1009 ([1c6bf7f](https://github.com/terrylica/data-source-manager/commit/1c6bf7f6a52dbc94fd391d4bf6b43f9098425467))
* **refactor:** Add script for safe file moving and import refactoring EL-1009 ([79a3203](https://github.com/terrylica/data-source-manager/commit/79a3203208d1101eb645b5a1f545bd95b6c13fa1))
* **release:** add mise-enabled semantic-release and prepare for public release ([1ab827f](https://github.com/terrylica/data-source-manager/commit/1ab827f0138a049b796428a6c1ad1bc4683b2942))
* remove async-related code and refactor for maintainability EL-1009 ([ca3b733](https://github.com/terrylica/data-source-manager/commit/ca3b733ed8d39168d40e55af73e2481bf2f78fa4))
* Remove HTTP client abstraction roadmap documentation EL-1009 ([b47ee1e](https://github.com/terrylica/data-source-manager/commit/b47ee1e2c590b7f927193d1989dd0ddc968470e7))
* Remove Vision API benchmark script EL-1009 ([d334fdb](https://github.com/terrylica/data-source-manager/commit/d334fdbfdc078ee5b3b39768a3f163009adaf8db))
* **reports:** add generated market data reports from verification scripts EL-1009 ([c849908](https://github.com/terrylica/data-source-manager/commit/c849908d34694f111948253f342f054d829f3847))
* resolve gaps between VISION API data and enforce schema alignment EL-1009 ([24a1fe4](https://github.com/terrylica/data-source-manager/commit/24a1fe4d8d5108d03ed82562773e1325afddd21e))
* **rules:** add error handling and FCP protocol context rules ([9451589](https://github.com/terrylica/data-source-manager/commit/9451589bce9c09df5328ac53d7f39f74edbc39b2))
* **rules:** Add rule for modularizing large files and isolating helpers EL-1009 ([8127ea6](https://github.com/terrylica/data-source-manager/commit/8127ea6fbdb12a8594d4aa59c8e4a0144234fd4d))
* **run_tests_parallel:** Improve usability and add help system EL-1009 ([9dbe523](https://github.com/terrylica/data-source-manager/commit/9dbe523f3198bd039d3857136f3eb20d450ec883))
* **run_tests.sh:** update script to handle interval-based test folders EL-1009 ([5d1a1f9](https://github.com/terrylica/data-source-manager/commit/5d1a1f9b9d0f168417e817da1bfbd1bef8456fca))
* **scripts,docs,test:** enhance dev experience with test runner & migration docs EL-1009 ([1113af2](https://github.com/terrylica/data-source-manager/commit/1113af2fef53cbeedf8cebb1ed88b11d22cf6352))
* **scripts/binance_vision_api_aws_s3:** Add XLMUSDT to spot synchronal report EL-1009 ([dfee02f](https://github.com/terrylica/data-source-manager/commit/dfee02f80f8b0b5e35a2b83d29c94ebb91415efe))
* **scripts/dev:** Add check-ruff command and centralize Ruff checks EL-1009 ([648ad21](https://github.com/terrylica/data-source-manager/commit/648ad213dcf0b6bba08a0ae6608f139687257099))
* **scripts/dev:** Add script to remove empty directories EL-1009 ([29f936d](https://github.com/terrylica/data-source-manager/commit/29f936d312bcb5ef3e2f42da7c4213d87cad0a9c))
* **scripts:** add code2prompt installation script with example generation EL-1009 ([154a4b0](https://github.com/terrylica/data-source-manager/commit/154a4b01f20a9a694a437c5e8f81112bf485f2db))
* **scripts:** add interactive dependency installation prompt EL-1009 ([7942471](https://github.com/terrylica/data-source-manager/commit/7942471962bace24dd8530e8e7206dbf0de1fbb4))
* **scripts:** add tools for verifying and fetching Binance historical data EL-1009 ([6cbc501](https://github.com/terrylica/data-source-manager/commit/6cbc5019bfa31cf0ea3dee4113d4ae1ddc0ed03a))
* **scripts:** Add Warnings Summary mode and improve command logging EL-1009 ([ffe62cb](https://github.com/terrylica/data-source-manager/commit/ffe62cbbb0b22f2d1c0efcb16f138c16ab4e5d26))
* **scripts:** Enhance test script for maintainability and asyncio config EL-1009 ([c540e0a](https://github.com/terrylica/data-source-manager/commit/c540e0a9233d50e44a9d43ccf4a11eca67116d0c))
* **scripts:** enhance validation to check argument-hint, allowed-tools, adr ([7f51c5a](https://github.com/terrylica/data-source-manager/commit/7f51c5a9511296f6346c7e0d72d938587267b9b1))
* **scripts:** improve Binance Vision API data downloader reliability and reporting EL-1009 ([3bbb293](https://github.com/terrylica/data-source-manager/commit/3bbb293358707aaa5033a4ba0b8d36a6214b9ab6))
* **scripts:** improve date terminology and file naming consistency EL-1009 ([13cf661](https://github.com/terrylica/data-source-manager/commit/13cf661c5634f4d55feacccba7cf53c731997b7b))
* **settings:** add extraKnownMarketplaces for cc-skills plugin ([4332eec](https://github.com/terrylica/data-source-manager/commit/4332eec510e455fc5a087b5deb669d97bfb77558))
* **settings:** add project settings.json with permission rules ([62362e2](https://github.com/terrylica/data-source-manager/commit/62362e2f040e22698d4642f6000d6dc8bbb59a70))
* **skills:** add $ARGUMENTS usage and user-invocable flag ([a9331bf](https://github.com/terrylica/data-source-manager/commit/a9331bf642cbedb7318f4323ade3cb08a40c0d48))
* **skills:** add adr field and evolution-log pattern to dsm-fcp-monitor ([4a13c82](https://github.com/terrylica/data-source-manager/commit/4a13c82030c40933c51fae62b9616bcd49608b80))
* **skills:** add allowed-tools for permission-free tool access ([4defe22](https://github.com/terrylica/data-source-manager/commit/4defe22d8208d0528f1dd7d65b96cc84bd4fc889))
* **skills:** add Claude Code skills for DSM usage and testing ([943c4d2](https://github.com/terrylica/data-source-manager/commit/943c4d2ea41dafb59981cadc6ac37bc805a6b261))
* **skills:** add FCP monitor skill and hooks configuration ([085a331](https://github.com/terrylica/data-source-manager/commit/085a331e94a5ccad13c91459b040169601c7bcfe))
* **skills:** add helper scripts for DSM operations ([1a2cf40](https://github.com/terrylica/data-source-manager/commit/1a2cf40f56610072b2445b19a9da98ad20dabdf3))
* **skills:** add workflow checklist pattern and official quality checklist ([73418b7](https://github.com/terrylica/data-source-manager/commit/73418b758e09240fdb03be57376ec5ca99d4fc7f))
* **skills:** enhance skills with advanced frontmatter patterns ([e95b3a4](https://github.com/terrylica/data-source-manager/commit/e95b3a44fcb3d2f34acdae72a154a6a26e47e7bb))
* **structure:** add CLAUDE.md, ADRs, and polyglot monorepo patterns ([5a7aecd](https://github.com/terrylica/data-source-manager/commit/5a7aecd0c2980e2aa3c20641adbbc048266094ab))
* **sync:** add partial days cache optimization to FCP mechanism EL-1009 ([dd06ca4](https://github.com/terrylica/data-source-manager/commit/dd06ca406a0b2ad6edbafdedd1af6c78c942c741))
* **sync:** integrate FSSpecVisionHandler and FCP caching mechanism EL-1009 ([a05789e](https://github.com/terrylica/data-source-manager/commit/a05789ea3c18746251683a6609942b04ef4a51c3))
* **task_management:** enhance cancellation and cleanup logic for long-term maintainability EL-1009 ([66cf511](https://github.com/terrylica/data-source-manager/commit/66cf511a63954d999ae65771d7b285fac3b5e1c4))
* **task-cancellation:** implement event-based cancellation and cleanup EL-1009 ([ad181f9](https://github.com/terrylica/data-source-manager/commit/ad181f9001fab39e7650e9fc587630903774225a))
* **test:** add clear screen option to run_tests_parallel.sh EL-1009 ([89437a0](https://github.com/terrylica/data-source-manager/commit/89437a01fd1a0db8750b8d40a040fe39ad2f5bd0))
* **testing:** add cache clearing utility for FCP-PM testing EL-1009 ([af63b90](https://github.com/terrylica/data-source-manager/commit/af63b900c7c6698e1039a00b92cf5bd89e18d587))
* **testing:** enhance test framework for parallel execution EL-1009 ([6570641](https://github.com/terrylica/data-source-manager/commit/6570641371f3f10a96a7c9d42f6ff948a1a25368))
* **tests/okx:** add comprehensive tests for OKX market data endpoints EL-1009 ([ec7fa29](https://github.com/terrylica/data-source-manager/commit/ec7fa29aed39b06b17186abe04d5c9fded5939fd))
* **tests/okx:** Add tests for OKX API candle and history endpoints EL-1009 ([4c7d061](https://github.com/terrylica/data-source-manager/commit/4c7d06158d974372a56895cce1735327ba779235))
* **tests:** add interval and market type tests EL-1009 ([378ce30](https://github.com/terrylica/data-source-manager/commit/378ce30ae903c673e5884e7edee3773e99ab71b6))
* **tests:** organize tests by interval, preparing for interval diversity EL-1009 ([18e0204](https://github.com/terrylica/data-source-manager/commit/18e0204221500934329d91375aa057da0d63a8e7))
* **tests:** Refactor cache optimization tests for direct function usage and improve mocking EL-1009 ([c085d86](https://github.com/terrylica/data-source-manager/commit/c085d8620be16bad284eaee01cb0bd3311a06d0f))
* **test:** Summarize test consolidation and fix remove script exit code EL-1009 ([27f9e6c](https://github.com/terrylica/data-source-manager/commit/27f9e6cae9cdd20e7d3a99d7c64099a86b515fe3)), closes [hi#level](https://github.com/hi/issues/level)
* **timeout-testing:** add comprehensive DataFrame visualization output EL-1009 ([afa72f8](https://github.com/terrylica/data-source-manager/commit/afa72f85a3c40b105473496769aec4bd28466401))
* **tools:** Configure vulture for dead code analysis and update ruff rules EL-1009 ([b614cc5](https://github.com/terrylica/data-source-manager/commit/b614cc5d2238ed07fc6f5d54ff923bebabb5c957))
* Update cache filename format to YYYYMMDD EL-1009 ([e6eaa1c](https://github.com/terrylica/data-source-manager/commit/e6eaa1c84642843f51fd3ea0303ea1876acc3229))
* update earliest dates for spot, um, and cm markets with new USDC and USDT pairs EL-1009 ([f29066b](https://github.com/terrylica/data-source-manager/commit/f29066b5edf99fa6e1be8b463b650afcac9a67fe))
* **utils:** add checksum verification and file handling utilities for Binance Vision API EL-1009 ([fde053b](https://github.com/terrylica/data-source-manager/commit/fde053bc64c1b998bc8ec46223b07cae52af48c1))
* **utils:** deprecate get_logger in favor of logger singleton EL-1009 ([b9626f6](https://github.com/terrylica/data-source-manager/commit/b9626f62a778521989a1bb1958eba9b137af4e65))
* **vision_data_client:** enhance timestamp validation and error handling EL-1009 ([7108a5c](https://github.com/terrylica/data-source-manager/commit/7108a5ca23cf260ace72140d79a9734ea890eaca)), closes [hi#frequency](https://github.com/hi/issues/frequency)
* **vision:** add checksum content length logging with truncation EL-1009 ([88d70c8](https://github.com/terrylica/data-source-manager/commit/88d70c81d64c5a70614aad9081389aee9e1b3a71))
* **VisionDataClient:** remove deprecated caching methods and update doc strings EL-1009 ([0b77ce5](https://github.com/terrylica/data-source-manager/commit/0b77ce583e12c86e2fec0988e176e49d3f52f25e))
* **vision:** enhance timestamp validation and format detection EL-1009 ([d5e4111](https://github.com/terrylica/data-source-manager/commit/d5e411181e48959c9ab28ef4504f239921998dd4))


### Performance Improvements

* **benchmark:** replace requests with httpx for improved performance EL-1009 ([6d090c0](https://github.com/terrylica/data-source-manager/commit/6d090c03a55cd59805f48759e23062792c82ef59))
* **benchmarks:** implement multi-tool download comparison suite EL-1009 ([f70638f](https://github.com/terrylica/data-source-manager/commit/f70638f40181405c7fba1e1ee8af6f4733d87297))
* **fcp-demo:** add performance metrics and timing instrumentation EL-1009 ([d046865](https://github.com/terrylica/data-source-manager/commit/d046865f90034d7190b65a76ad38ccd64ceb88ac))
* **logging:** downgrade logger level in core and utils modules from INFO to DEBUG EL-1009 ([b693d3a](https://github.com/terrylica/data-source-manager/commit/b693d3add7b5f0050329158a76a5733a9dec0de5))
* **vision_data_client:** replace zipfile and tempfile with fsspec for faster ZIP handling EL-1009 ([3695d67](https://github.com/terrylica/data-source-manager/commit/3695d6796ae0d22373521e51132b82e294e175a5))


### Reverts

* **devcontainer:** Revert github cli feature from devcontainer EL-1009 ([398aef4](https://github.com/terrylica/data-source-manager/commit/398aef44da816ac5bab07f485980863469d5e356))


### BREAKING CHANGES

* **release:** License changed from Proprietary to MIT

SRED-Type: support-work
SRED-Claim: DSM-RELEASE
* **debug:** Remove graceful fallbacks, implement strict fail-fast debugging

Philosophy Implementation:
✅ FAIL-FAST with maximum debug information (no failovers/failsafes)
✅ Timezone-aware logging eliminates datetime confusion
✅ Rich exception context for rapid debugging
✅ Aggressive validation with informative error messages

New Features:
- TimezoneDebugError with structured context dictionaries
- _format_timezone_info() shows timezone details: UTC+0000, NAIVE warnings
- Timezone mismatch detection between filter times and data timestamps
- Boundary violation detection with precise violation magnitude
- Data loss detection at exact timestamp boundaries
- Emoji-enhanced logging for visual debugging (🕐 🎯 📊 ⚠️ ✅ 🚨)

Enhanced Functions:
- trace_dataframe_timestamps(): Rich timezone-aware timestamp tracing
- analyze_filter_conditions(): FAIL-FAST filter condition analysis
- compare_filtered_results(): Boundary violation and data loss detection

Integration:
- Removed try/catch graceful degradation from time_utils.py
- Direct imports ensure debug utilities are always available
- Designed for TiRex Context Stability experiments with DSM

This eliminates timezone confusion in extended backtesting scenarios
by providing immediate, detailed feedback on timestamp inconsistencies.
* **time-utils:** The filter_dataframe_by_time function now includes the end time
in results rather than excluding it. Test cases have been updated to reflect this
behavior change.

The key improvements:
- Created a single, consistent pattern for time boundaries that applies universally
- Ensured that documentation and implementation match in their behavior
- Simplified mental model by using inclusive boundaries in both start and end times
- Eliminated edge cases where exclusive boundaries created inconsistent results

This approach follows the Liskov Substitution Principle by establishing a unified
abstraction that works consistently regardless of whether filtering by timestamp
column, open_time column, or dataframe index.

Thursday 2025-04-03 22:17:27 Vancouver GMT-07:00
