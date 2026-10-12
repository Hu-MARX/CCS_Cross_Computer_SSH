# CCS contributor instructions

Keep CCS as a generic execution layer. Scientific routes, model implementations,
training data and conclusions belong to the calling project.

- Read action contracts before changing CLI/SDK/MCP behavior. Keep schemas,
  capability negotiation, examples and guides synchronized.
- Preserve strict SSH host-key checking, exact dry-run confirmation, durable
  mutation identities and unknown-outcome semantics. Never retry a mutation
  automatically or substitute another worker/idempotency key.
- Do not add real worker addresses, keys, private configuration, runtime data,
  checkpoints or personal run records to this repository.
- Run only relevant tests while editing; at delivery use `scripts/verify.py`
  or `scripts/verify-local.ps1` with the appropriate group. Avoid unnecessary
  repeated full suites. Keep optional training skips and native platform checks
  separate from completed validation.
- No remote deployment, training, cancellation or failure injection is authorized
  by cloning the repository or running its local verification. Use a separately
  authorized isolated environment for remote acceptance.
