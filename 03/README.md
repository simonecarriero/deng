# Dev

## Install dependencies
```
uv sync
```

## Lint and format
```
uv run ruff check --fix
uv run ruff format
```

# Local run

## Configure
Add the values for `GOOGLE_PROJECT`, `GOOGLE_BUCKET` and `GOOGLE_APPLICATION_CREDENTIALS` to the `.env` file

## Infra

```
terraform apply --var="project=<google-project>" --var="credentials=<google-application-credentials>"
```

## Start dagster

```
uv run dagster dev -f orchestration.py
```
