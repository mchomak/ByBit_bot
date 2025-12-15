# CONTRIBUTING Guide

Thank you for your interest in the project! Below is how to get started, what we accept and how, and the general rules.

## How to get started
1. Fork the repository and create a branch from `main`:
`git checkout -b feat/<short-about-feature>`
2. Install the dependencies and configure the environment ('.env`).

## Stack and requirements
- Python 3.12+
- PostgreSQL 13+
- Docker (optional)
- Linter/formatter: **Ruff** + **Black**
- Tests: **pytest** (+ **pytest-mock**, **hypothesis** if necessary)

## Code style
- Formatting: `black .`
- Lint: `ruff .`
- Imports: organize (ruff/isort), avoid cyclical dependencies.
- Types: `typing`/`pydantic' is welcome (where appropriate).

## Commit messages
Following the **Conventional Commits**:
```
<type>(optional scope): <short summary>

[body]
[footer(s)]
``
Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `chore`, `build`, `ci`.  
Learn more: https://www.conventionalcommits.org/

## Branches and PR
- Branches: `feat/*`, `fix/*`, `docs/*`, `chore/*'.
- Open the PR in the `main`; add a description of the changes, screenshots/logs, if necessary.
- All CI checks must pass (linters, tests).
- The volume of PR is reasonable; major changes are broken down.

## Testing
Minimum for PR:
- Unit tests for new logic.
- Integration tests if you are affecting the network/database/chain (with mockups).
- e2e (dry‑run) for pipeline changes.
Launch:
``bash
pytest -q
```

## Documentation
- Update the README and/or `docs/` when changing interfaces/CLI/configs.
- Use `MarkdownV2`/`HTML` in Telegram messages and keep them short.

## Releases and Changelog
- We are leading `CHANGELOG.md ` according to the principles of **Keep a Changelog** and specify the SemVer.
- The release includes: git tags, binary/model artifacts (if necessary), updated migrations.

## Security
- Do not publish private keys/DSN/tokens.
- To report a security issue: write to `security@example.com ` (replace it with your contact).

## Code of Conduct
The project uses **Contributor Covenant v2.1**. See `CODE_OF_CONDUCT.md `.