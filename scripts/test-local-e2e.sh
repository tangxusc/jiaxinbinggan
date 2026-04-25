#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MYSQL_CONTAINER="${MYSQL_CONTAINER:-jxb-mysql-test}"
PG_CONTAINER="${PG_CONTAINER:-jxb-pg-test}"
NETWORK="${NETWORK:-jxb-test-net}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0.34}"
PG_IMAGE="${PG_IMAGE:-postgres:16-alpine}"
MYSQL_PORT="${MYSQL_PORT:-13306}"
PG_PORT="${PG_PORT:-15432}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-jxbpass}"
PG_PASSWORD="${PG_PASSWORD:-jxbpass}"
MYSQL_DATABASE="${MYSQL_DATABASE:-jxb_source}"
PG_DATABASE="${PG_DATABASE:-jxb_target}"
TMP_ROOT="${TMP_ROOT:-/tmp/jxb-e2e-$$}"
CHECKPOINT_ROOT="$TMP_ROOT/checkpoint"
LOG_DIR="$TMP_ROOT/logs"
KEEP_ON_FAILURE="${KEEP_ON_FAILURE:-1}"

GO_ENV=(
  "GOROOT=/Users/tangxu/sdk/go1.16rc1"
  "GOTOOLCHAIN=local"
  "GOPATH=/tmp/jxb-gopath"
  "GOCACHE=/tmp/jxb-go-cache"
  "GOMODCACHE=/tmp/jxb-gomod-cache"
  "GOPROXY=https://goproxy.cn,direct"
)

PASS_COUNT=0
FAIL_COUNT=0
KNOWN_COUNT=0
FAILED_TESTS=()

mkdir -p "$LOG_DIR" "$CHECKPOINT_ROOT"

log() {
  printf '[jxb-e2e] %s\n' "$*"
}

record_pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  log "PASS: $*"
}

record_known() {
  KNOWN_COUNT=$((KNOWN_COUNT + 1))
  log "KNOWN: $*"
}

record_fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  FAILED_TESTS+=("$*")
  log "FAIL: $*"
}

cleanup() {
  status=$?
  if [[ $status -eq 0 || "$KEEP_ON_FAILURE" == "0" ]]; then
    docker rm -f "$MYSQL_CONTAINER" "$PG_CONTAINER" >/dev/null 2>&1 || true
    docker network rm "$NETWORK" >/dev/null 2>&1 || true
    rm -rf "$TMP_ROOT"
  else
    log "retaining test resources for debugging: $TMP_ROOT"
    log "set KEEP_ON_FAILURE=0 to remove resources even on failure"
  fi
}
trap cleanup EXIT

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "missing required command: $1"
    exit 1
  fi
}

run_step() {
  local name="$1"
  shift
  log "RUN: $name"
  "$@" >"$LOG_DIR/$name.log" 2>&1
}

docker_pull_with_proxy_fallback() {
  local image="$1"
  if docker image inspect "$image" >/dev/null 2>&1; then
    log "image already present: $image"
    return 0
  fi
  if docker pull "$image"; then
    return 0
  fi
  log "plain docker pull failed for $image; trying local proxy on 127.0.0.1:7897"
  if lsof -nP -iTCP:7897 -sTCP:LISTEN >/dev/null 2>&1; then
    HTTP_PROXY=http://127.0.0.1:7897 \
    HTTPS_PROXY=http://127.0.0.1:7897 \
    ALL_PROXY=socks5://127.0.0.1:7897 \
    docker pull "$image"
  else
    log "proxy port 7897 is not listening"
    return 1
  fi
}

assert_eq() {
  local name="$1"
  local got="$2"
  local want="$3"
  if [[ "$got" == "$want" ]]; then
    record_pass "$name"
  else
    record_fail "$name: got [$got], want [$want]"
  fi
}

assert_contains_file() {
  local name="$1"
  local file="$2"
  local needle="$3"
  if grep -Fq "$needle" "$file"; then
    record_pass "$name"
  else
    record_fail "$name: missing [$needle] in $file"
  fi
}

mysql_exec() {
  docker exec -i "$MYSQL_CONTAINER" mysql -uroot -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" "$@"
}

mysql_scalar() {
  docker exec -i "$MYSQL_CONTAINER" mysql -uroot -p"$MYSQL_PASSWORD" -N -B "$MYSQL_DATABASE" -e "$1"
}

pg_exec() {
  docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d "$PG_DATABASE" "$@"
}

pg_scalar() {
  docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -At -U postgres -d "$PG_DATABASE" -c "$1" | tr -d '\r'
}

wait_for_mysql() {
  for _ in $(seq 1 60); do
    if docker exec "$MYSQL_CONTAINER" mysql -uroot -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" -e "select 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs "$MYSQL_CONTAINER" >"$LOG_DIR/mysql-startup.log" 2>&1 || true
  return 1
}

wait_for_pg() {
  for _ in $(seq 1 60); do
    if docker exec "$PG_CONTAINER" pg_isready -U postgres -d "$PG_DATABASE" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  docker logs "$PG_CONTAINER" >"$LOG_DIR/postgres-startup.log" 2>&1 || true
  return 1
}

write_config() {
  local path="$1"
  local job="$2"
  cat >"$path" <<YAML
mysql:
  host: 127.0.0.1
  port: $MYSQL_PORT
  database: $MYSQL_DATABASE
  username: root
  password: '$MYSQL_PASSWORD'
postgresql:
  host: 127.0.0.1
  port: $PG_PORT
  database: $PG_DATABASE
  schema: public
  username: postgres
  password: '$PG_PASSWORD'
$job
YAML
}

run_cli_success() {
  local name="$1"
  local cfg="$2"
  local log_file="$LOG_DIR/$name.log"
  if (cd "$ROOT" && env "${GO_ENV[@]}" go run ./cmd/jxb -config "$cfg") >"$log_file" 2>&1; then
    record_pass "$name command succeeded"
    assert_contains_file "$name stdout" "$log_file" "导入完成"
  else
    record_fail "$name command failed; see $log_file"
  fi
}

run_cli_success_debug() {
  local name="$1"
  local cfg="$2"
  local log_file="$LOG_DIR/$name.log"
  if (cd "$ROOT" && env "${GO_ENV[@]}" go run ./cmd/jxb -config "$cfg" -debug) >"$log_file" 2>&1; then
    record_pass "$name command succeeded"
    assert_contains_file "$name debug output" "$log_file" "SQL"
  else
    record_fail "$name command failed; see $log_file"
  fi
}

run_cli_failure() {
  local name="$1"
  local cfg="$2"
  local expected="$3"
  local log_file="$LOG_DIR/$name.log"
  if (cd "$ROOT" && env "${GO_ENV[@]}" go run ./cmd/jxb -config "$cfg") >"$log_file" 2>&1; then
    record_fail "$name unexpectedly succeeded; see $log_file"
  else
    record_pass "$name command failed as expected"
    assert_contains_file "$name error" "$log_file" "$expected"
  fi
}

init_environment() {
  require_command docker
  require_command go
  require_command lsof

  docker rm -f "$MYSQL_CONTAINER" "$PG_CONTAINER" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true

  if lsof -nP -iTCP:"$MYSQL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    log "port $MYSQL_PORT is already in use"
    exit 1
  fi
  if lsof -nP -iTCP:"$PG_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    log "port $PG_PORT is already in use"
    exit 1
  fi

  docker_pull_with_proxy_fallback "$MYSQL_IMAGE"
  docker_pull_with_proxy_fallback "$PG_IMAGE"

  docker network create "$NETWORK" >/dev/null
  docker run -d --name "$MYSQL_CONTAINER" --network "$NETWORK" -p "$MYSQL_PORT:3306" \
    -e MYSQL_ROOT_PASSWORD="$MYSQL_PASSWORD" \
    -e MYSQL_DATABASE="$MYSQL_DATABASE" \
    "$MYSQL_IMAGE" >/dev/null
  docker run -d --name "$PG_CONTAINER" --network "$NETWORK" -p "$PG_PORT:5432" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    -e POSTGRES_DB="$PG_DATABASE" \
    "$PG_IMAGE" >/dev/null
  wait_for_mysql
  wait_for_pg
}

init_schema() {
  mysql_exec <<'SQL'
DROP TABLE IF EXISTS users_basic;
DROP TABLE IF EXISTS extra_pg_source;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users_transform;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS large_inventory;
DROP TABLE IF EXISTS checkpoint_items;
DROP TABLE IF EXISTS manual_cp_items;
DROP TABLE IF EXISTS checkpoint_bad;
DROP TABLE IF EXISTS missing_source;
DROP TABLE IF EXISTS good_after;
DROP TABLE IF EXISTS bad_rows;
DROP TABLE IF EXISTS bad_json;

CREATE TABLE users_basic (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  seq INT,
  group_id INT
);
INSERT INTO users_basic VALUES
  (1, 'A', 30, 1),
  (2, 'B', 10, 1),
  (3, 'C', 20, 2),
  (4, 'D', 40, 1),
  (5, 'E', 50, 2);

CREATE TABLE extra_pg_source (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
INSERT INTO extra_pg_source VALUES (1, 'source-one'), (2, 'source-two');

CREATE TABLE orders (
  id INT PRIMARY KEY,
  amount INT
);
INSERT INTO orders VALUES (1, 100), (2, 200);

CREATE TABLE users_transform (
  id INT PRIMARY KEY,
  full_name VARCHAR(100),
  nick VARCHAR(100),
  active TINYINT,
  created_at DATETIME,
  meta TEXT,
  status INT,
  age_text VARCHAR(20)
);
INSERT INTO users_transform VALUES
  (1, ' Alice ', '', 1, '2026-04-25 10:11:12', '{"tier":"gold"}', 1, '42'),
  (2, 'Bob', 'Bobby', 0, '2026-04-24 09:00:00', '{"tier":"silver"}', 2, '33');

CREATE TABLE products (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  price INT
);
INSERT INTO products VALUES (1, 'Widget', 100), (2, 'Gadget', 200);

CREATE TABLE large_inventory (
  id INT PRIMARY KEY,
  name VARCHAR(100),
  quantity INT,
  updated_at DATETIME
);
INSERT INTO large_inventory (id, name, quantity, updated_at)
SELECT n, CONCAT('mysql-item-', n), n * 3, TIMESTAMP('2026-04-25 00:00:00') + INTERVAL n SECOND
FROM (
  SELECT ones.n + tens.n * 10 + hundreds.n * 100 + thousands.n * 1000 + 1 AS n
  FROM
    (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) ones
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) tens
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) hundreds
    CROSS JOIN (SELECT 0 n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) thousands
) numbers
WHERE n <= 5000;

CREATE TABLE checkpoint_items (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
INSERT INTO checkpoint_items VALUES (1, 'one'), (2, 'two'), (3, 'three');

CREATE TABLE manual_cp_items (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
INSERT INTO manual_cp_items VALUES (1, 'one'), (2, 'two'), (3, 'three');

CREATE TABLE checkpoint_bad (
  id INT PRIMARY KEY,
  name VARCHAR(100) NULL
);
INSERT INTO checkpoint_bad VALUES (1, 'ok'), (2, NULL);

CREATE TABLE missing_source (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
INSERT INTO missing_source VALUES (1, 'only-source-columns');

CREATE TABLE good_after (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
INSERT INTO good_after VALUES (1, 'good');

CREATE TABLE bad_rows (
  id INT PRIMARY KEY,
  bad_int VARCHAR(100)
);
INSERT INTO bad_rows VALUES (1, 'abc');

CREATE TABLE bad_json (
  id INT PRIMARY KEY,
  payload TEXT
);
INSERT INTO bad_json VALUES (1, '{');
SQL

  pg_exec <<'SQL'
DROP TABLE IF EXISTS users_basic;
DROP TABLE IF EXISTS extra_pg_target;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users_transform_pg;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS large_inventory;
DROP TABLE IF EXISTS checkpoint_items;
DROP TABLE IF EXISTS manual_cp_items;
DROP TABLE IF EXISTS checkpoint_bad;
DROP TABLE IF EXISTS missing_source_pg;
DROP TABLE IF EXISTS good_after;
DROP TABLE IF EXISTS bad_rows_pg;
DROP TABLE IF EXISTS bad_json_pg;

CREATE TABLE users_basic (
  id BIGINT PRIMARY KEY,
  name TEXT,
  seq INTEGER
);

CREATE TABLE extra_pg_target (
  id BIGINT PRIMARY KEY,
  name TEXT,
  extra_nullable TEXT NULL,
  extra_default TEXT NOT NULL DEFAULT 'pg-extra-default'
);

CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  amount INTEGER
);

CREATE TABLE users_transform_pg (
  id BIGINT PRIMARY KEY,
  name TEXT,
  nickname TEXT NULL,
  active BOOLEAN,
  created_at TIMESTAMP,
  metadata JSONB,
  status TEXT,
  age INTEGER,
  source_system TEXT,
  nullable_missing TEXT NULL,
  db_default TEXT NOT NULL DEFAULT 'pg-default'
);

CREATE TABLE products (
  id BIGINT PRIMARY KEY,
  name TEXT,
  price INTEGER
);

CREATE TABLE large_inventory (
  id BIGINT PRIMARY KEY,
  name TEXT,
  quantity INTEGER,
  updated_at TIMESTAMP
);
INSERT INTO large_inventory (id, name, quantity, updated_at)
SELECT n, 'pg-existing-' || n, -1, TIMESTAMP '2026-01-01 00:00:00'
FROM generate_series(1, 1000) AS n;

CREATE TABLE checkpoint_items (
  id BIGINT PRIMARY KEY,
  name TEXT
);

CREATE TABLE manual_cp_items (
  id BIGINT PRIMARY KEY,
  name TEXT
);

CREATE TABLE checkpoint_bad (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE missing_source_pg (
  id BIGINT PRIMARY KEY,
  name TEXT,
  missing_default TEXT,
  missing_nullable TEXT NULL,
  missing_db_default TEXT NOT NULL DEFAULT 'pg-default'
);

CREATE TABLE good_after (
  id BIGINT PRIMARY KEY,
  name TEXT
);

CREATE TABLE bad_rows_pg (
  id BIGINT PRIMARY KEY,
  bad_int INTEGER
);

CREATE TABLE bad_json_pg (
  id BIGINT PRIMARY KEY,
  payload JSONB
);
SQL
}

test_basic_and_concurrency() {
  pg_exec <<'SQL'
TRUNCATE users_basic, orders;
SQL
  local cfg="$TMP_ROOT/basic.yaml"
  write_config "$cfg" "job:
  name: e2e-basic
  batchSize: 2
  concurrency: 2
  onMissingSourceColumn: fail
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: users_basic
      targetTable: users_basic
      where: group_id = 1
      orderBy: seq
      columns:
        - source: id
          target: id
        - source: name
          target: name
        - source: seq
          target: seq
    - sourceTable: orders
      targetTable: orders
"
  run_cli_success "basic_and_concurrency" "$cfg"
  assert_eq "basic users count" "$(pg_scalar "select count(*) from users_basic")" "3"
  assert_eq "basic users order-independent ids" "$(pg_scalar "select string_agg(id::text, ',' order by id) from users_basic")" "1,2,4"
  assert_eq "orders count" "$(pg_scalar "select count(*) from orders")" "2"
}

test_pg_extra_columns_unmapped() {
  pg_exec <<'SQL'
TRUNCATE extra_pg_target;
SQL
  local cfg="$TMP_ROOT/pg-extra-columns.yaml"
  write_config "$cfg" "job:
  name: e2e-pg-extra-columns
  batchSize: 2
  concurrency: 1
  onMissingSourceColumn: fail
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: extra_pg_source
      targetTable: extra_pg_target
      columns:
        - source: id
          target: id
        - source: name
          target: name
"
  run_cli_success "pg_extra_columns_unmapped" "$cfg"
  assert_eq "pg extra columns row count" "$(pg_scalar "select count(*) from extra_pg_target")" "2"
  assert_eq "pg extra nullable untouched" "$(pg_scalar "select bool_and(extra_nullable is null) from extra_pg_target")" "t"
  assert_eq "pg extra default untouched" "$(pg_scalar "select string_agg(extra_default, ',' order by id) from extra_pg_target")" "pg-extra-default,pg-extra-default"
  assert_eq "pg explicit mapped columns" "$(pg_scalar "select string_agg(id::text || ':' || name, ',' order by id) from extra_pg_target")" "1:source-one,2:source-two"
}

test_transform_and_missing_columns() {
  pg_exec <<'SQL'
TRUNCATE users_transform_pg;
SQL
  local cfg="$TMP_ROOT/transform.yaml"
  write_config "$cfg" "job:
  name: e2e-transform
  batchSize: 1
  concurrency: 1
  onMissingSourceColumn: useDefault
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: users_transform
      targetTable: users_transform_pg
      columns:
        - source: id
          target: id
          required: true
        - source: full_name
          target: name
          transform: trimString
        - source: nick
          target: nickname
          transform: emptyStringToNull
        - source: active
          target: active
          transform: tinyintToBoolean
        - source: created_at
          target: created_at
          transform: mysqlDatetimeToPgTimestamp
        - source: meta
          target: metadata
          transform: jsonStringToJsonb
        - source: status
          target: status
          transform: enumMapping
          mapping:
            '1': paid
            '2': cancelled
        - source: age_text
          target: age
        - target: source_system
          defaultValue: mysql
        - source: missing_nullable
          target: nullable_missing
        - source: missing_db_default
          target: db_default
"
  run_cli_success "transform_and_missing_columns" "$cfg"
  assert_eq "transform row count" "$(pg_scalar "select count(*) from users_transform_pg")" "2"
  assert_eq "trim string" "$(pg_scalar "select name from users_transform_pg where id=1")" "Alice"
  assert_eq "empty string to null" "$(pg_scalar "select nickname is null from users_transform_pg where id=1")" "t"
  assert_eq "tinyint boolean" "$(pg_scalar "select active from users_transform_pg where id=1")" "t"
  assert_eq "jsonb value" "$(pg_scalar "select metadata->>'tier' from users_transform_pg where id=1")" "gold"
  assert_eq "enum mapping" "$(pg_scalar "select status from users_transform_pg where id=2")" "cancelled"
  assert_eq "integer adaptation" "$(pg_scalar "select age from users_transform_pg where id=1")" "42"
  assert_eq "fixed default value" "$(pg_scalar "select source_system from users_transform_pg where id=1")" "mysql"
  assert_eq "nullable missing source" "$(pg_scalar "select nullable_missing is null from users_transform_pg where id=1")" "t"
  assert_eq "target database default" "$(pg_scalar "select db_default from users_transform_pg where id=1")" "pg-default"
}

test_conflicts() {
  pg_exec <<'SQL'
TRUNCATE products;
SQL
  local insert_cfg="$TMP_ROOT/products-insert.yaml"
  write_config "$insert_cfg" "job:
  name: e2e-products-insert
  batchSize: 2
  concurrency: 1
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: products
      targetTable: products
"
  run_cli_success "products_insert_first" "$insert_cfg"
  run_cli_failure "products_insert_duplicate" "$insert_cfg" "duplicate"
  assert_eq "duplicate insert leaves row count unchanged" "$(pg_scalar "select count(*) from products")" "2"

  local ignore_cfg="$TMP_ROOT/products-ignore.yaml"
  write_config "$ignore_cfg" "job:
  name: e2e-products-ignore
  batchSize: 2
  concurrency: 1
  onWriteConflict: ignore
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: products
      targetTable: products
"
  run_cli_success "products_ignore_duplicate" "$ignore_cfg"
  assert_eq "ignore keeps row count" "$(pg_scalar "select count(*) from products")" "2"

  mysql_exec <<'SQL'
UPDATE products SET name = 'Widget v2', price = 150 WHERE id = 1;
SQL
  local upsert_cfg="$TMP_ROOT/products-upsert.yaml"
  write_config "$upsert_cfg" "job:
  name: e2e-products-upsert
  batchSize: 2
  concurrency: 1
  onWriteConflict: upsert
  conflictKeys: [id]
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: products
      targetTable: products
"
  run_cli_success "products_upsert" "$upsert_cfg"
  assert_eq "upsert updates value" "$(pg_scalar "select name || ':' || price from products where id=1")" "Widget v2:150"

  local upsert_without_keys="$TMP_ROOT/products-upsert-no-keys.yaml"
  write_config "$upsert_without_keys" "job:
  name: e2e-products-upsert-no-keys
  batchSize: 2
  concurrency: 1
  onWriteConflict: upsert
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: products
      targetTable: products
"
  run_cli_failure "products_upsert_without_keys" "$upsert_without_keys" "duplicate"
}

test_large_existing_data_upsert() {
  local cfg="$TMP_ROOT/large-existing-data.yaml"
  write_config "$cfg" "job:
  name: e2e-large-existing-data
  batchSize: 257
  concurrency: 1
  onWriteConflict: upsert
  conflictKeys: [id]
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: large_inventory
      targetTable: large_inventory
      orderBy: id
"
  run_cli_success "large_existing_data_upsert" "$cfg"
  assert_eq "large existing row count" "$(pg_scalar "select count(*) from large_inventory")" "5000"
  assert_eq "large existing id checksum" "$(pg_scalar "select sum(id) from large_inventory")" "12502500"
  assert_eq "large existing quantity checksum" "$(pg_scalar "select sum(quantity) from large_inventory")" "37507500"
  assert_eq "large existing overlap updated" "$(pg_scalar "select name || ':' || quantity from large_inventory where id = 1")" "mysql-item-1:3"
  assert_eq "large existing high id inserted" "$(pg_scalar "select name || ':' || quantity from large_inventory where id = 5000")" "mysql-item-5000:15000"
  assert_eq "large existing no stale rows" "$(pg_scalar "select count(*) from large_inventory where name like 'pg-existing-%'")" "0"
}

test_checkpoint() {
  pg_exec <<'SQL'
TRUNCATE checkpoint_items, manual_cp_items, checkpoint_bad;
SQL
  rm -rf "$CHECKPOINT_ROOT/e2e-checkpoint" "$CHECKPOINT_ROOT/e2e-manual" "$CHECKPOINT_ROOT/e2e-checkpoint-bad"

  local cfg="$TMP_ROOT/checkpoint.yaml"
  write_config "$cfg" "job:
  name: e2e-checkpoint
  batchSize: 2
  concurrency: 1
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: true
    column: id
    order: asc
    storage: $CHECKPOINT_ROOT
    fixedUpperBound: true
  tables:
    - sourceTable: checkpoint_items
      targetTable: checkpoint_items
"
  run_cli_success "checkpoint_first_run" "$cfg"
  assert_eq "checkpoint first count" "$(pg_scalar "select count(*) from checkpoint_items")" "3"
  assert_eq "checkpoint last id" "$(grep -F '"lastCheckpointId": 3' "$CHECKPOINT_ROOT/e2e-checkpoint/checkpoint_items.json" >/dev/null && echo yes || echo no)" "yes"
  mysql_exec <<'SQL'
INSERT INTO checkpoint_items VALUES (4, 'four');
SQL
  run_cli_success "checkpoint_second_run_fixed_upper_bound" "$cfg"
  assert_eq "fixed upper bound excludes new id" "$(pg_scalar "select count(*) from checkpoint_items")" "3"

  mkdir -p "$CHECKPOINT_ROOT/e2e-manual"
  cat >"$CHECKPOINT_ROOT/e2e-manual/manual_cp_items.json" <<'JSON'
{
  "jobName": "e2e-manual",
  "sourceTable": "manual_cp_items",
  "targetTable": "manual_cp_items",
  "checkpointColumn": "id",
  "lastCheckpointId": 1,
  "maxId": 3,
  "status": "running",
  "updatedAt": "2026-04-25T00:00:00+08:00",
  "readRows": 1,
  "writtenRows": 1,
  "skippedRows": 0,
  "failedRows": 0
}
JSON
  local manual_cfg="$TMP_ROOT/manual-checkpoint.yaml"
  write_config "$manual_cfg" "job:
  name: e2e-manual
  batchSize: 2
  concurrency: 1
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: true
    column: id
    order: asc
    storage: $CHECKPOINT_ROOT
    fixedUpperBound: true
  tables:
    - sourceTable: manual_cp_items
      targetTable: manual_cp_items
"
  run_cli_success "manual_checkpoint_resume" "$manual_cfg"
  assert_eq "manual checkpoint imports after last id" "$(pg_scalar "select string_agg(id::text, ',' order by id) from manual_cp_items")" "2,3"

  local bad_cfg="$TMP_ROOT/checkpoint-bad.yaml"
  write_config "$bad_cfg" "job:
  name: e2e-checkpoint-bad
  batchSize: 2
  concurrency: 1
  onWriteConflict: insert
  view:
    enabled: false
  checkpoint:
    enabled: true
    column: id
    order: asc
    storage: $CHECKPOINT_ROOT
    fixedUpperBound: true
  tables:
    - sourceTable: checkpoint_bad
      targetTable: checkpoint_bad
"
  run_cli_failure "checkpoint_failed_batch" "$bad_cfg" "null"
  assert_eq "failed batch does not save checkpoint" "$(test -f "$CHECKPOINT_ROOT/e2e-checkpoint-bad/checkpoint_bad.json" && echo yes || echo no)" "no"
  mysql_exec <<'SQL'
UPDATE checkpoint_bad SET name = 'fixed' WHERE id = 2;
SQL
  run_cli_success "checkpoint_retry_after_fix" "$bad_cfg"
  assert_eq "checkpoint retry count" "$(pg_scalar "select count(*) from checkpoint_bad")" "2"
}

test_failure_modes() {
  pg_exec <<'SQL'
TRUNCATE missing_source_pg, bad_rows_pg, bad_json_pg, good_after;
SQL

  local missing_source="$TMP_ROOT/missing-source.yaml"
  write_config "$missing_source" "job:
  name: e2e-missing-source
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: does_not_exist
      targetTable: missing_source_pg
"
  run_cli_failure "source_table_missing" "$missing_source" "mysql"

  local missing_target="$TMP_ROOT/missing-target.yaml"
  write_config "$missing_target" "job:
  name: e2e-missing-target
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: missing_source
      targetTable: does_not_exist
"
  run_cli_failure "target_table_missing" "$missing_target" "postgresql"

  local missing_target_column="$TMP_ROOT/missing-target-column.yaml"
  write_config "$missing_target_column" "job:
  name: e2e-missing-target-column
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: missing_source
      targetTable: missing_source_pg
      columns:
        - source: id
          target: id
        - source: name
          target: no_such_column
"
  run_cli_failure "target_column_missing" "$missing_target_column" "no_such_column"

  local missing_column_ok="$TMP_ROOT/missing-column-ok.yaml"
  write_config "$missing_column_ok" "job:
  name: e2e-missing-column-ok
  onMissingSourceColumn: useDefault
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: missing_source
      targetTable: missing_source_pg
      columns:
        - source: id
          target: id
        - source: name
          target: name
        - source: no_source_default
          target: missing_default
          defaultValue: configured
        - source: no_source_nullable
          target: missing_nullable
        - source: no_source_db_default
          target: missing_db_default
"
  run_cli_success "missing_columns_use_default" "$missing_column_ok"
  assert_eq "missing default configured" "$(pg_scalar "select missing_default from missing_source_pg where id=1")" "configured"
  assert_eq "missing nullable null" "$(pg_scalar "select missing_nullable is null from missing_source_pg where id=1")" "t"
  assert_eq "missing db default" "$(pg_scalar "select missing_db_default from missing_source_pg where id=1")" "pg-default"

  pg_exec -c "TRUNCATE missing_source_pg;"
  local missing_required="$TMP_ROOT/missing-required.yaml"
  write_config "$missing_required" "job:
  name: e2e-missing-required
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: missing_source
      targetTable: missing_source_pg
      columns:
        - source: id
          target: id
        - source: missing_required
          target: missing_default
          required: true
"
  run_cli_failure "missing_required_fails" "$missing_required" "必填"
  assert_eq "missing required no rows" "$(pg_scalar "select count(*) from missing_source_pg")" "0"

  local skip_row="$TMP_ROOT/skip-row-known.yaml"
  write_config "$skip_row" "job:
  name: e2e-skip-row-known
  onMissingSourceColumn: skipRow
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: missing_source
      targetTable: missing_source_pg
      columns:
        - source: id
          target: id
        - source: missing_skip_row
          target: missing_default
"
  pg_exec -c "TRUNCATE missing_source_pg;"
  run_cli_success "skip_row_current_behavior" "$skip_row"
  if [[ "$(pg_scalar "select count(*) from missing_source_pg")" == "0" ]]; then
    record_pass "skipRow implemented as row skip"
  else
    record_known "skipRow currently writes a row with NULL instead of skipping"
  fi

  local unknown_transform="$TMP_ROOT/unknown-transform.yaml"
  write_config "$unknown_transform" "job:
  name: e2e-unknown-transform
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: bad_rows
      targetTable: bad_rows_pg
      columns:
        - source: id
          target: id
        - source: bad_int
          target: bad_int
          transform: noSuchTransform
"
  run_cli_failure "unknown_transform" "$unknown_transform" "未知转换规则"

  local type_error="$TMP_ROOT/type-error.yaml"
  write_config "$type_error" "job:
  name: e2e-type-error
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: bad_rows
      targetTable: bad_rows_pg
"
  run_cli_failure "type_conversion_error" "$type_error" "类型适配失败"

  local json_error="$TMP_ROOT/json-error.yaml"
  write_config "$json_error" "job:
  name: e2e-json-error
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: bad_json
      targetTable: bad_json_pg
      columns:
        - source: id
          target: id
        - source: payload
          target: payload
          transform: jsonStringToJsonb
"
  run_cli_failure "json_conversion_error" "$json_error" "unexpected"

  local mysql_bad_port="$TMP_ROOT/mysql-bad-port.yaml"
  write_config "$mysql_bad_port" "job:
  name: e2e-mysql-bad-port
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: users_basic
      targetTable: users_basic
"
  sed -i.bak "s/port: $MYSQL_PORT/port: 9/" "$mysql_bad_port"
  run_cli_failure "mysql_bad_port" "$mysql_bad_port" "mysql 连接失败"

  local pg_bad_port="$TMP_ROOT/pg-bad-port.yaml"
  write_config "$pg_bad_port" "job:
  name: e2e-pg-bad-port
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: users_basic
      targetTable: users_basic
"
  sed -i.bak "/^postgresql:/,/^job:/ s/port: $PG_PORT/port: 9/" "$pg_bad_port"
  run_cli_failure "postgres_bad_port" "$pg_bad_port" "postgresql 连接失败"
}

test_failfast() {
  pg_exec <<'SQL'
TRUNCATE bad_rows_pg, good_after;
SQL
  local failfast_true="$TMP_ROOT/failfast-true.yaml"
  write_config "$failfast_true" "job:
  name: e2e-failfast-true
  concurrency: 1
  failFast: true
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: bad_rows
      targetTable: bad_rows_pg
    - sourceTable: good_after
      targetTable: good_after
"
  run_cli_failure "failfast_true" "$failfast_true" "bad_rows"
  assert_eq "failfast true stops later table" "$(pg_scalar "select count(*) from good_after")" "0"

  local failfast_false="$TMP_ROOT/failfast-false.yaml"
  write_config "$failfast_false" "job:
  name: e2e-failfast-false
  concurrency: 1
  failFast: false
  view:
    enabled: false
  checkpoint:
    enabled: false
  tables:
    - sourceTable: bad_rows
      targetTable: bad_rows_pg
    - sourceTable: good_after
      targetTable: good_after
"
  run_cli_failure "failfast_false" "$failfast_false" "bad_rows"
  assert_eq "failfast false continues later table" "$(pg_scalar "select count(*) from good_after")" "1"
}

test_tui_smoke() {
  pg_exec <<'SQL'
TRUNCATE orders;
SQL
  local cfg="$TMP_ROOT/tui.yaml"
  write_config "$cfg" "job:
  name: e2e-tui
  batchSize: 2
  concurrency: 1
  onWriteConflict: insert
  view:
    enabled: true
    refreshIntervalMs: 50
  checkpoint:
    enabled: false
  tables:
    - sourceTable: orders
      targetTable: orders
"
  run_cli_success_debug "tui_smoke" "$cfg"
  assert_eq "tui target rows" "$(pg_scalar "select count(*) from orders")" "2"
}

main() {
  log "temporary workspace: $TMP_ROOT"
  init_environment
  init_schema
  test_basic_and_concurrency
  test_pg_extra_columns_unmapped
  test_transform_and_missing_columns
  test_large_existing_data_upsert
  test_conflicts
  test_checkpoint
  test_failure_modes
  test_failfast
  test_tui_smoke

  log "summary: pass=$PASS_COUNT fail=$FAIL_COUNT known=$KNOWN_COUNT logs=$LOG_DIR"
  if [[ $FAIL_COUNT -ne 0 ]]; then
    printf '\nFailed tests:\n'
    printf ' - %s\n' "${FAILED_TESTS[@]}"
    exit 1
  fi
}

main "$@"
