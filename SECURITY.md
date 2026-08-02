# Security policy

## Supported versions

This project is pre-1.0. Only the latest published `0.0.x` release line receives security fixes. Pin an exact version and upgrade promptly.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

Use GitHub's private vulnerability reporting on this repository (the **Security** tab, then **Report a vulnerability**). If that is unavailable, email the maintainers at oss@laserdata.com.

Include the affected crate and version, a description, and a reproduction if you have one. We aim to acknowledge a report within a few business days and will then coordinate a fix and a disclosure timeline with you.

The wire crate decodes untrusted bytes and is held to a never-panic guarantee backed by a robustness suite and fuzzing, so a decode crash or panic on hostile input is in scope.
