// Package obs is walkie's observability surface: structured logging, metrics
// and health.
//
// Owning work item:
//
//	eka get walkie/ts:observability-baseline
//
// # The metric that is not optional
//
// The direct-versus-relay ratio must exist from the MVP onward, even though it
// necessarily reads zero until phase 2 delivers the direct data plane.
//
// This is the mitigation for the one substantive disagreement recorded in this
// project's planning. adr:001-transport-topology chose a hybrid topology —
// attempt direct, fall back to relaying through the coordinator — over the
// coordinator-only alternative. The failure mode of that choice is silence: if
// direct paths never form, a hybrid design keeps working while quietly relaying
// everything, and the central architectural decision becomes unobservable in
// production. Without this counter, nobody finds out.
//
// See ses:planning-2026-08-20 for how that decision was taken.
//
// # Constraints
//
//   - Bind metrics and health to the tailnet interface only, never 0.0.0.0.
//     adr:004-security-model applies to these endpoints exactly as it does to
//     the message endpoint, and ts:coordinator-skeleton requires it be verified
//     by test rather than by inspection.
//   - No metric label may carry a device identity in a form that turns the
//     metrics endpoint into a presence side channel.
//   - Logs carry no message content and no key material.
package obs
