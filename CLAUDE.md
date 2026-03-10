# CLAUDE.md - Repository Guide for AI Agents

## What is this repo?
RoadDB is the managed database product landing page and configuration for BlackRoad OS.
It's a static site deployed via GitHub Pages.

## Repository structure
- `index.html` - Main product page (PostgreSQL, MongoDB, Redis)
- `organization-schema.json` - Schema.org structured data
- `sitemap.xml` - Product sitemap for SEO
- `robots.txt` - Crawler directives
- `BRAND_IDENTITY.md` - Brand disambiguation (BlackRoad != BlackRock)
- `.github/workflows/` - CI/CD automation

## Workflows
- `autonomous-agent.yml` - Validates HTML/JSON/XML on PRs, auto-merges passing PRs
- `deploy-pages.yml` - Deploys to GitHub Pages on push to master
- `manage-issues.yml` - Auto-labels issues, closes spam, closes stale issues weekly

## Key rules
- Default branch is `master` (not main)
- This is a static site - no build step, no dependencies
- All validation is file-based (HTML well-formedness, JSON/XML parsing)
- BlackRoad OS, Inc. is NOT BlackRock - always maintain brand disambiguation
- Proprietary license - see LICENSE file
