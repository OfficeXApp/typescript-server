# Standalone Typescript Server | OfficeX

```sh
# build and run
$ docker compose up --build -d

# view logs
$ docker compose logs -f
```

Test it

```sh
$ curl http://localhost:8888/health

$ curl -X POST http://localhost:8888/v1/factory/api_keys/upsert \
  -H "Content-Type: application/json" \
  -d '{"action": "CREATE", "name": "Test Key"}'
```

Poke around in SQLite in Docker

```sh
$ docker ps
$ docker exec -it c63d516867b6 /bin/bash


app@3a9a55d2fe95:/data/organizations/te/test-org$ ls
database.db
app@3a9a55d2fe95:/data/organizations/te/test-org$ sqlite3 database.db
SQLite version 3.40.1 2022-12-28 14:03:47
Enter ".help" for usage hints.
sqlite> .tables
_sqlx_migrations       contact_labels         contacts
contact_groups         contact_past_user_ids  superswap_history
sqlite> .quit
```

Shut down docker containers, even volumes

```sh
$ docker compose down
$ docker compose down --volumes
```
