# Decisions

## Mid-run events are queued for the next run

An event that arrives while a run is in progress waits in the inbox and is picked up by the next run. Runs never change shape mid-way, which keeps crash recovery simple: a resumed run replays exactly the steps it saved.

## One inbox per agent

Each agent has a single inbox and a single worker. Events carry a `subject` field, so per-user inboxes can be added later without changing the event format.

## MIT licence

The project is released under the MIT licence.
