# Powietrze

Narzędzie CLI do importu danych jakości powietrza do bazy PostgreSQL.

## Instalacja

```bash
uv sync
```

## Użycie

### Inicjalizacja bazy danych

```bash
uv run powietrze init-db --db-url "postgresql://user:pass@localhost/powietrze"
```

### Import danych

```bash
# Import jednego archiwum
uv run powietrze import dane/2024.zip --db-url "postgresql://user:pass@localhost/powietrze"

# Import wielu archiwów
uv run powietrze import dane/2024.zip dane/2023.zip --db-url "..."

# Import z verbose
uv run powietrze import dane/2024.zip --db-url "..." --verbose
```

### Statystyki

```bash
uv run powietrze stats --db-url "postgresql://user:pass@localhost/powietrze"
```

## Zmienna środowiskowa

Zamiast podawać `--db-url` przy każdym wywołaniu, możesz ustawić zmienną środowiskową:

```bash
export DATABASE_URL="postgresql://user:pass@localhost/powietrze"
uv run powietrze import dane/2024.zip
```

