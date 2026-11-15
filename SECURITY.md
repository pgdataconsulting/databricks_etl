# Security policy

## Reporting a vulnerability

Please report security problems privately, not in a public issue: open the
repository's **Security** tab and choose **Report a vulnerability**. Include
the affected version, what an attacker can do, and the smallest configuration
or input that shows it.

Once a fix is available it is released as a patch version and the advisory is
published with credit to the reporter, unless you prefer otherwise.

## Supported versions

Fixes are made on the latest minor release.

| Version | Supported |
| --- | --- |
| 1.x | yes |
| < 1.0 | no |

## What the framework treats as trusted

Knowing this is what makes a report actionable:

- **Pipeline configurations are code.** A YAML file can read and write any
  data the job's identity can reach and can contain SQL expressions. They
  belong in version control behind review, and the directory a job reads them
  from must not be writable by people who may not act as that identity.
- **Runtime parameters are trusted input.** They are substituted as text into
  the configuration, including into SQL expressions. Values must come from
  the orchestrator, never from an end user.
- **Secrets are never trusted to stay out of error messages by themselves**,
  which is why the framework masks them. A way to make a resolved secret
  appear in a log line, run summary, audit table or raised error is a
  vulnerability; please report it.

The full model, including identities, grants and the supply chain, is in
[docs/security.md](docs/security.md).
