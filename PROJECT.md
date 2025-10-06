# Project Name

JOBservitor

---

## 📋 Task Board

### Backlog

- [ ] full integration tests that run both scheduler and executors
- [ ] Executors should register themselves with the scheduler
- [ ] Executors can receive messages from the schedulers
- [ ] Examine the LIST command for jobs. it relies on the queues, but may be insufficient.
- [ ] Replace redis persistence layer with something more fun, that will take load off redis for performance
- [ ] Add option for job timeout
- [ ] Add docker compose scaffolding to start everything together
- [ ] Add responsible signal handling for executor
- [ ] Make sure jobs are effectively distributed across executors, rather than having one executor dominate things
- [ ] Add executor introspection to identify the system they are
- [ ] Executor should update job status, somehow
- [ ] Executor dequeue logic should be rewritten into LUA to have proper atomicity and reduce job dequeue lag

### In Progress

- [ ] Job aborting

### Done

- [x] Run Docker jobs
- [x] Executor should respect job requirements
- [x] A job should track what executor is working on it (IP, etc)
- [x] Executors can update jobs and mark them as running
- [x] Executors should monitor the ANY queue as well
- [x] Split the job queue by architecture
- [x] Executors can pick jobs off their queue
- [x] Create initial executors
- [x] Initial fastapi scheduler, uv, test suite
- [x] Define the initial job structure and plumb CRUD into scheduler
- [x] Plumb redis saving and enqueue for jobs
- [x] Remove default GPU arch as an option, turns out GPU arch can be not a requirement
