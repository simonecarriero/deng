# Dev

## Install dependencies
```
uv sync
```

## Run tests
```
uv run pytest
```

## Lint and format
```
uv run check --fix
uv run format
```

# Local run

## Start postgres
```
docker run \
  -e POSTGRES_USER=root \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=ny_taxi \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 postgres:17
```

## Start dagster

```
uv run dagster dev -f orchestration.py
```
