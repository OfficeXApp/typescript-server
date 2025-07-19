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

Amazon EC2 Linux Pre-Setup

```sh

#!/bin/bash
# Update the system
sudo yum update -y

# Install Git
sudo yum install git -y

# Install Docker (for Amazon Linux 2023)
# For Amazon Linux 2, use: sudo amazon-linux-extras install docker -y
sudo yum install docker -y

# Start the Docker service
sudo service docker start

# Enable Docker to start on boot
sudo systemctl enable docker

# Add the ec2-user to the docker group so you can run Docker commands without sudo
sudo usermod -a -G docker ec2-user

# (Optional) Reboot for group changes to take effect immediately
# A new SSH session would also pick up the changes without a reboot
# sudo reboot

# install docker-compose
$ sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.5/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
$ sudo chmod +x /usr/local/bin/docker-compose
```

Run in production:

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
