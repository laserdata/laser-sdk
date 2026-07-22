# Native streaming

Publishes a keyed record with an exact `uint16` header, then a configurable
batch workload. Two consumer groups drain the same log: one uses automatic
commits and one commits only after successful handling.

```bash
npm run build
node dist/src/native-streaming/main.js
```

`LASER_MESSAGES` and `LASER_BATCH` scale the run. The example uses raw Apache
Iggy and needs no managed capability.
