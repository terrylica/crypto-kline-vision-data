EMERGENCY RESTORE COMMANDS:
=========================
Current HEAD: 57216c2b707cb2244a9d185ab41a7fefdfe7e639
Current Branch: main

# To completely undo all changes:
git checkout main
git reset --hard 57216c2b707cb2244a9d185ab41a7fefdfe7e639
git clean -fd

# To revert specific files:
git checkout 57216c2b707cb2244a9d185ab41a7fefdfe7e639 -- utils/market_constraints.py
git checkout 57216c2b707cb2244a9d185ab41a7fefdfe7e639 -- core/sync/data_source_manager.py

# Current working directory files backed up to:
/home/tca/eon/nt/repos/data-source-manager/backup_20250813_203731/
