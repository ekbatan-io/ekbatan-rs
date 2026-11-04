// The guided path, in order. One list: the index renders it, the pager walks it, and adding a
// page is a line here plus the file (and its docs/learn/ twin).
export const LEARN_PAGES = [
    { slug: "getting-started", title: "Getting started", blurb: "Start here. Follow one wallet change through the dual-write problem, model events, action staging, and what the executor commits." },
    { slug: "the-dual-write-trap", title: "The dual-write trap", blurb: "Why writing to a database and to a broker separately creates silent drift, and why the event has to be stored with the state that caused it." },
    { slug: "outbox", title: "The outbox", blurb: "How domain rows and event rows commit together, so the database holds both the state and the fact that produced it, and the ways to use the events afterwards." },
    { slug: "models-and-entities", title: "Models and entities", blurb: "Models emit events when they change; entities do not. Which to use, why both are immutable, and what the #[model] and #[entity] macros write for you." },
    { slug: "actions", title: "Actions", blurb: "Build the new state first, then let the executor commit domain rows and event rows in one transaction. Retries, errors, and what an action may not do." },
    { slug: "your-first-action", title: "Your first action", blurb: "Build a wallet deposit end to end: the event, the model, the action, the call that runs it, and the event row you never wrote." },
    { slug: "blocking-or-async", title: "Blocking or async?", blurb: "Two APIs over one engine: ekbatan::blocking and ekbatan::aio. How to pick, what is the same in both, and the one mistake to avoid." },
    { slug: "wiring-a-service", title: "Wiring a service", blurb: "Every object a service needs, built from one config file and closed in the right order, with a separate connection pool for each kind of work." },
    { slug: "consuming-events", title: "Consuming events", blurb: "Use committed events after the fact: handle them in the same service with the local event handler, or stream them out. Every path is at least once." },
    { slug: "sharding", title: "Adding a shard", blurb: "Take a single-database service to two shards. Actions, queries and endpoints stay the same; the model's id carries its shard." },
    { slug: "sagas", title: "Sagas", blurb: "Split a multi-step workflow into committed actions chained by events, with a compensation when a later step fails and nothing to roll back." },
    { slug: "going-to-production", title: "Going to production", blurb: "Migrations at deploy, TLS, pools, shutdown, what to watch in traces and metrics, and what ekbatan does not do for you yet." },
] as const;
