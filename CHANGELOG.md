# Changelog

## 2.0.0

- Replaced internal queue implementation with [`rasa/queue`](https://hexdocs.pm/rasa/)
- Fixed shutdown and exit to drain waiting callers with `ConnectionUnavailable`
  instead of leaving them blocked forever
- Fixed shutdown and exit to close active (checked-out) connections, cancel
  their deadline timers, and remove their monitors
