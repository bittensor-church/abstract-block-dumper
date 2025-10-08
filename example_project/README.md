# Demo 

This is a demonstration of the abstract_block_dumper package.

## How it works

The package uses decorators to register block processing tasks. These tasks can be scheduled to run at specific intervals or conditions (e.g., every block, every N blocks, on epoch start/end).

## Running the Example via docker compose

1. Build and start the services:
```bash
docker-compose up --build
```
2. Access the Django admin interface at `http://localhost:8000/admin` with username `admin` and password `admin` (automatically created).
3. Start the block dumper scheduler:
```bash
docker-compose exec web python manage.py block_tasks
```
