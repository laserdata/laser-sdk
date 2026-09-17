# Security policy

## Supported versions

This project is pre-1.0. Security fixes target the latest published release. Pin an exact version and update promptly.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Use GitHub's private vulnerability reporting on this repository (the Security tab, then Report a vulnerability). If that is unavailable, email the maintainers at oss@laserdata.com.

Include the crate, version, problem description, and reproduction when available. Maintainers aim to acknowledge reports within a few business days. They then coordinate the fix and disclosure with the reporter.

The wire crate decodes untrusted input. Decode crashes and panics are security-reporting concerns. Deterministic tests and fuzzing cover malformed input.
