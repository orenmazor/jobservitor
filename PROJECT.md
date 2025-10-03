# Project Name

JOBservitor

---

## 📋 Task Board

### Backlog

- [ ] Create initial executors
- [ ] Executors can pick jobs off their queue
- [ ] Executors can update jobs and mark them as running
- [ ] Executors can receive messages from the schedulers
- [ ] Examine the LIST command for jobs. it relies on the queues, but may be insufficient.
- [ ] Replace redis persistence layer with something more fun, that will take load off redis for performance

### In Progress

- [ ] Split queues by architecture (why bother with executors monitoring jobs for others?)

### Done

- [x] Initial fastapi scheduler, uv, test suite
- [x] Define the initial job structure and plumb CRUD into scheduler
- [x] Plumb redis saving and enqueue for jobs
