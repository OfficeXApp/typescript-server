# Standalone Typescript Server | OfficeX

NodeJS Backend Server for [OfficeX](https://officex.app).
Anonymous OfficeX | Documents, Spreadsheets & Cloud Storage

Quickstart

```sh
$ git clone https://github.com/OfficeXApp/typescript-server.git
$ npm install
$ cp .env.example .env
$ npm run dev
```

Docker container available at [https://hub.docker.com/r/officex/typescript-server](https://hub.docker.com/r/officex/typescript-server)

## Environment Files

Copy the `.env.example` file to `.env` and fill in the values. If in development, you can leave the values exactly as is.

```.env
# .env.example

NODE_ENV=development # Set to 'development' for local development, 'production' for production

# Mandatory Defaults
DATA_DIR=./data # Recommended: Do not change this. The directory where the server will store its data
PORT=8888 # Recommended: Do not change this. The port the server will listen on
LOG_LEVEL=silent # Recommended: Do not change this. The log level for reporting

# Mandatory for production
TRAEFIK_ENABLED=false # True if Traefik should handle SSL (eg. AWS EC2), false if localhost http or a 3rd party handles SSL (eg. Cloudflare/RepoCloud.io)
LE_EMAIL=youremail@email.com # Your email for Let's Encrypt notifications (or empty for dev). only necessary if TRAEFIK_ENABLED=true
SERVER_DOMAIN=yourdomain.com # Your domain name, eg. myapp.com.  only necessary if TRAEFIK_ENABLED=true

# Optional
SANITY_CHECK_ENV=helloworld # Optional: A sanity check environment variable which you can see at GET /health
OWNER= # Optional: The owner of the server as OfficeX UserID, eg. UserID_..., If omited, server will create an admin
```

## Development:

```sh
# build and run
$ docker compose up --build -d

# view logs
$ docker compose logs -f

# clear and restart, wipe volumes
$ docker compose down --volumes

# refresh without wiping volumes
$ docker compose down && docker compose up --build -d && docker compose logs -f

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

Convinence hard restart (wipes out volumes)

```sh
$ docker compose down --volumes && docker compose up --build -d && docker compose logs -f
```

## Testing

```sh
npm install -g newman

newman run postman/postman-rest-api.json
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
# create the .env file
$ vi .env

# Deploy (pull image from registry, run containers)
# Ensure .env.prod is configured and EC2 Security Group/DNS are set.
$ docker-compose -f docker-compose.prod.yml up --pull always -d

# View logs
$ docker-compose -f docker-compose.prod.yml logs -f

# Stop and restart (preserves data/volumes)
$ docker-compose -f docker-compose.prod.yml down && docker-compose -f docker-compose.prod.yml up --pull always -d

# Clear and restart, wipe volumes (DANGER: DELETES ALL DATA AND CERTS!)
$ docker-compose -f docker-compose.prod.yml down --volumes
```

Push update to production:

```sh
$ docker buildx build --platform linux/amd64 -t officex/typescript-server:latest .
$ docker push officex/typescript-server:latest
```
