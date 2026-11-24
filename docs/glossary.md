# Glossary

These terms are used consistently in the code, docs and CLI.

- **Agent:** a configured worker with a system prompt, tools and a budget. It lives indefinitely and acts only when woken by an event.
- **Event:** something that happened and may need the agent's attention, such as a webhook delivery, a fired timer or an approval answer. Every event has an idempotency key, so the same delivery is stored only once.
- **Inbox:** the queue of events waiting for one agent. Each agent has one inbox and one worker.
- **Run:** one wake-up of an agent. A run claims the oldest pending event in the inbox, works on it and ends. An agent has at most one unfinished run, and events that arrive during a run wait for the next run.
- **Step:** one unit of work inside a run, such as a model call or a tool call. Steps are saved as they happen.
- **Transcript:** the ordered list of a run's saved steps. A run that crashes resumes from its transcript.
- **Timer:** a scheduled future event, either one-off or recurring. When it is due, the scheduler puts an event in the agent's inbox.
- **Schedule:** a recurring timer defined in an agent's configuration rather than set by the agent. Schedules are synced on startup: new ones are added and removed ones cancelled.
- **Outbox item:** a message the agent has decided to send. Items are delivered by the dispatcher, never directly by the agent.
- **Channel:** a named place an agent can send messages, such as a webhook or a file. Channels are set in the agent's configuration; the agent picks one by name and never chooses the destination itself.
- **Approval:** a human decision on a tool call marked as needing approval. The run pauses until someone approves or denies the call through the API or the CLI, then resumes. While it waits, the agent keeps handling its other events.
- **Task:** a goal that takes several runs, broken into items, with free-form notes and a log of what was done. An event whose subject is `task:<id>` loads the task into its run, and timers set during that run are attached to the task.
- **Memory:** notes an agent keeps across all its runs, as markdown files under `/memories`: preferences, facts about people and systems, lessons, and pointers to where things live. Every run is shown `/memories/INDEX.md` and a list of the other files.
- **Standing instructions:** `/instructions.md`, written only by the agent's operator through the API or CLI and shown at the top of every run. Unlike memory, the agent can't change it.
- **Trusted source:** an event source, such as `user`, whose runs may write memory freely. Memory writes during runs from any other source wait for approval.
- **Budget:** the per-agent caps on tokens, cost and action rate. Crossing a cap stops the run.
