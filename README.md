# Standalone Typescript Server | OfficeX

## Development:

```sh
# build and run
$ docker compose up --build -d

# view logs
$ docker compose logs -f

# clear and restart, wipe volumes
$ docker compose down --volumes

# or restart fresh
$ docker compose down --volumes && docker compose up --build -d && docker compose logs -f
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
$ docker exec -it typescript-server-app-1 /bin/bash


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

Convinence restart

```sh
$ docker compose down --volumes && docker compose up --build -d && docker compose logs -f
```

## Production

```sh
# Deploy (pull image from registry, run containers)
# Ensure .env.prod is configured and EC2 Security Group/DNS are set.
$ docker compose -f docker-compose.prod.yml up --pull always -d

# View logs
$ docker compose -f docker-compose.prod.yml logs -f

# Stop and restart (preserves data/volumes)
$ docker compose -f docker-compose.prod.yml down && docker compose -f docker-compose.prod.yml up --pull always -d

# Clear and restart, wipe volumes (DANGER: DELETES ALL DATA AND CERTS!)
$ docker compose -f docker-compose.prod.yml down --volumes
```
