# Agent Guidelines

## User Stories

USER_STORIES.md exists only in projects that people use directly: a web app, admin panel, CLI or desktop app. A library like this one has none: README.md describes what the package does, how to install and use it, and its main flows. Do not add USER_STORIES.md for library changes; if the package gains direct users, such as a CLI people run, ask your handler whether to start one.

## General Workflow

- Prefer existing patterns used in this repository when designing and implementing new features.
- Keep documentation claims tied to code that exists in the current tree.
- Before marking a task as done, make sure tests and linters {% if cookiecutter.ci_use_typechecker %} and type checks {% endif %} pass. The general README.md contains instructions for running these checks.
- Follow the rules described in [engineering-standards.md](engineering-standards.md). Each task should be explicitly checked for correctness in adherence to the engineering standards.

## Local instructions

After reading this file, read `AGENTS.local.md` if it exists.

`AGENTS.local.md` may contain checkout-specific commands, environment details, and personal workflow preferences. It must not weaken or contradict the repository-wide requirements in this file and in case of a conflict, always default to the repository-wide rules and engineering standards.
