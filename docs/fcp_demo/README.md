# FCP Demo CLI Documentation

Generated on: 2025-04-18 00:38:38.156

## Overview

This documentation was automatically generated from the Typer CLI help text.

## Command Line Interface

```console
Usage: fcp_demo.py [OPTIONS]                                                                                                                                                                                                               
                                                                                                                                                                                                                                            
 FCP Demo: Demonstrates the Failover Control Protocol (FCP) mechanism.                                                                                                                                                                      
 This script shows how DataSourceManager automatically retrieves data from different sources:                                                                                                                                               
                                                                                                                                                                                                                                            
 1. Cache (Local Arrow files)                                                                                                                                                                                                               
 2. VISION API                                                                                                                                                                                                                              
 3. REST API                                                                                                                                                                                                                                
                                                                                                                                                                                                                                            
 It displays real-time source information about where each data point comes from.                                                                                                                                                           
                                                                                                                                                                                                                                            
 Time Range Priority Hierarchy:                                                                                                                                                                                                             
                                                                                                                                                                                                                                            
 1. --days or -d flag (HIGHEST PRIORITY):                                                                                                                                                                                                   
   - If provided, overrides any --start-time and --end-time values                                                                                                                                                                          
   - Calculates range as                                                                                                                                                                                                                    
   - Example: --days 5 will fetch data from 5 days ago until now                                                                                                                                                                            
                                                                                                                                                                                                                                            
 2. --start-time and --end-time (SECOND PRIORITY):                                                                                                                                                                                          
   - Used only when BOTH are provided AND --days is NOT provided                                                                                                                                                                            
   - Defines exact time range to fetch data from                                                                                                                                                                                            
   - Example: --start-time 2025-04-10 --end-time 2025-04-15                                                                                                                                                                                 
                                                                                                                                                                                                                                            
 3. Default Behavior (FALLBACK):                                                                                                                                                                                                            
   - If neither of the above conditions are met                                                                                                                                                                                             
   - Uses default days=3 to calculate range as                                                                                                                                                                                              
                                                                                                                                                                                                                                            
 Sample Commands:                                                                                                                                                                                                                           
                                                                                                                                                                                                                                            
 Basic Usage:                                                                                                                                                                                                                               
   ./examples/dsm_sync_simple/fcp_demo.py                                                                                                                                                                                                   
   ./examples/dsm_sync_simple/fcp_demo.py --symbol ETHUSDT --market spot                                                                                                                                                                    
                                                                                                                                                                                                                                            
 Time Range Options (By Priority):                                                                                                                                                                                                          
   # PRIORITY 1: Using --days flag (overrides any start/end times)                                                                                                                                                                          
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -d 7                                                                                                                                                                                   
                                                                                                                                                                                                                                            
   # PRIORITY 2: Using start and end times (only if --days is NOT provided)                                                                                                                                                                 
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -st 2025-04-05T00:00:00 -et 2025-04-06T00:00:00                                                                                                                                        
                                                                                                                                                                                                                                            
   # FALLBACK: No time flags (uses default days=3)                                                                                                                                                                                          
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT                                                                                                                                                                                        
                                                                                                                                                                                                                                            
 Market Types:                                                                                                                                                                                                                              
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -m um                                                                                                                                                                                  
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSD_PERP -m cm                                                                                                                                                                              
                                                                                                                                                                                                                                            
 Different Intervals:                                                                                                                                                                                                                       
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 5m                                                                                                                                                                                  
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -i 1h                                                                                                                                                                                  
   ./examples/dsm_sync_simple/fcp_demo.py -s SOLUSDT -m spot -i 1s  -cc -l D -st 2025-04-14T15:31:01 -et 2025-04-14T15:32:01                                                                                                                
                                                                                                                                                                                                                                            
 Data Source Options:                                                                                                                                                                                                                       
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -es REST                                                                                                                                                                               
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -nc                                                                                                                                                                                    
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -cc                                                                                                                                                                                    
                                                                                                                                                                                                                                            
 Testing FCP Mechanism:                                                                                                                                                                                                                     
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -fcp                                                                                                                                                                                   
   ./examples/dsm_sync_simple/fcp_demo.py -s BTCUSDT -fcp -pc                                                                                                                                                                               
                                                                                                                                                                                                                                            
 Combined Examples:                                                                                                                                                                                                                         
   ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSDT -m um -i 15m -st 2025-04-01 -et 2025-04-10 -r 5 -l DEBUG                                                                                                                               
   ./examples/dsm_sync_simple/fcp_demo.py -s ETHUSD_PERP -m cm -i 5m -d 10 -fcp -pc -l D -cc                                                                                                                                                
                                                                                                                                                                                                                                            
╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --symbol           -s        TEXT                                           Trading symbol (e.g., BTCUSDT) [default: BTCUSDT]                                                                                                            │
│ --market           -m        [spot|um|cm|futures_usdt|futures_coin]         Market type: spot, um (USDT-M futures), cm (Coin-M futures) [default: spot]                                                                                  │
│ --interval         -i        TEXT                                           Time interval (e.g., 1m, 5m, 1h) [default: 1m]                                                                                                               │
│ --chart-type       -ct       [klines|fundingRate]                           Type of chart data [default: klines]                                                                                                                         │
│ --start-time       -st       TEXT                                           [SECOND PRIORITY] Start time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Used only if both --start-time AND --end-time are provided AND --days is NOT │
│                                                                             provided                                                                                                                                                     │
│                                                                             [default: None]                                                                                                                                              │
│ --end-time         -et       TEXT                                           [SECOND PRIORITY] End time in ISO format (YYYY-MM-DDTHH:MM:SS) or YYYY-MM-DD. Used only if both --start-time AND --end-time are provided AND --days is NOT   │
│                                                                             provided                                                                                                                                                     │
│                                                                             [default: None]                                                                                                                                              │
│ --days             -d        INTEGER                                        [HIGHEST PRIORITY] Number of days to fetch from current time. Overrides --start-time/--end-time if provided [default: 3]                                     │
│ --enforce-source   -es       [AUTO|REST|VISION]                             Force specific data source (default: AUTO) [default: AUTO]                                                                                                   │
│ --retries          -r        INTEGER                                        Maximum number of retry attempts [default: 3]                                                                                                                │
│ --no-cache         -nc                                                      Disable caching (cache is enabled by default)                                                                                                                │
│ --clear-cache      -cc                                                      Clear the cache directory before running                                                                                                                     │
│ --test-fcp         -fcp                                                     Run the special test for Failover Control Protocol (FCP) mechanism                                                                                           │
│ --prepare-cache    -pc                                                      Pre-populate cache with the first segment of data (only used with --test-fcp)                                                                                │
│ --gen-doc          -gd                                                      Generate Markdown documentation from Typer help into docs/fcp_demo/ directory                                                                                │
│ --gen-lint-config  -glc                                                     Generate markdown linting configuration files along with documentation (only used with --gen-doc)                                                            │
│ --log-level        -l        [DEBUG|INFO|WARNING|ERROR|CRITICAL|D|I|W|E|C]  Set the log level (default: INFO). Shorthand options: D=DEBUG, I=INFO, W=WARNING, E=ERROR, C=CRITICAL [default: INFO]                                        │
│ --help             -h                                                       Show this message and exit.                                                                                                                                  │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Documentation Generation Examples

For convenience, you can generate this documentation using:

```bash
# Generate this documentation
./examples/dsm_sync_simple/fcp_demo.py --gen-doc
./examples/dsm_sync_simple/fcp_demo.py -gd

# Generate documentation with linting configuration files
./examples/dsm_sync_simple/fcp_demo.py -gd -glc
```
