# Decisions

## Mid-run events are queued for the next run

An event that arrives while a run is in progress waits in the inbox and is picked up by the next run. Runs never change shape mid-way, which keeps crash recovery simple: a resumed run replays exactly the steps it saved.

## One inbox per agent

Each agent has a single inbox and a single worker. Events carry a `subject` field, so per-user inboxes can be added later without changing the event format.

## MIT licence

The project is released under the MIT licence.

## One event per run

Each run handles a single event. Starting a run claims its event in the same database transaction, so a crash can never leave an event claimed with no run to finish it. The database also allows at most one unfinished run per agent.

## Missed timer ticks fire once

If the process is down through several ticks of a recurring timer, it fires once when the process comes back, then moves to the next tick after the current time. This avoids a burst of catch-up runs after downtime.

## Timers carry their own time zone

A recurring timer's cron expression is read in the timer's time zone (default UTC), so "every day at 9:00" follows daylight saving changes. All times are stored in UTC.

## Messages are delivered at least once

A message is marked sent only after its channel accepts it. If the process crashes in between, the message is sent again on restart. Every delivery carries the message id as an `Idempotency-Key`, so receivers can drop repeats. The alternative, never resending, could silently lose messages.

## Agents send only to configured channels

An agent names a channel from its configuration and never supplies a URL or address. Where each channel delivers is set by whoever runs the agent, so a prompt injection cannot redirect the agent's messages.

## Paused runs don't block the agent

A run waiting for an approval is set aside rather than left running, so the agent keeps handling other events while a human decides. When the answer comes in, the paused run resumes by replaying its saved steps.

## The CLI talks to the running server

`double-agent approvals` commands call the server's HTTP API rather than writing to the database. Workers only wake when signalled, so a direct database write would not resume a paused run. Set `DOUBLE_AGENT_URL` or `--url` when the server is not on `http://127.0.0.1:8000`.

## Task memory is structured, shown as markdown

A task's items, statuses, notes and log are stored as rows in SQLite and shown to the agent as a markdown file. Markdown is what the model reads and writes best; the structure lets the runtime enforce rules a note can't, such as cancelling a task's follow-up timers when it closes. Each create or update is named after its run and step, so a resumed run never applies one twice.

## A run's task context is saved with the run

When a run starts on a task, the task as it looked at that moment is saved as the run's first step. A resumed run replays that snapshot, so the model sees an unchanged conversation even if the task was updated in between.
