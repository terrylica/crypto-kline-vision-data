# Internal Link Validation

Check all markdown links within docs/, CLAUDE.md files for validity. Use lychee or manual check to verify each link resolves to existing file/section. Run lychee --config .lychee.toml docs/ CLAUDE.md src/CLAUDE.md tests/CLAUDE.md. Check relative vs absolute link formats. Broadcast broken or incorrect links found.
