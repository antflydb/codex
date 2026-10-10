//! Relational Codex tables in Antfly's PostgreSQL-style SQL.
//!
//! Statements are written once, with `$1`-style parameters, and run either on
//! an embedded `.aflite` file through the Antfly SQLx driver or on a remote
//! `antfly standalone` through its PostgreSQL wire listener. Values cross the
//! boundary as [`SqlValue`] so stores are not generic over two SQLx drivers.

use std::sync::Arc;

use antfly_embedded::sqlx::Antfly as AntflyDb;
use antfly_embedded::sqlx::AntflyArguments;
use antfly_embedded::sqlx::AntflyConnectOptions;
use futures::future::BoxFuture;
use serde_json::Value;
use sqlx::Arguments;
use sqlx::AssertSqlSafe;
use sqlx::Column;
use sqlx::Row;
use sqlx::TypeInfo;
use sqlx::ValueRef;
use sqlx_postgres::PgArguments;
use sqlx_postgres::PgPoolOptions;
use sqlx_postgres::Postgres;

use crate::error::AntflyError;
use crate::error::AntflyResult;

/// One SQL parameter or result cell.
#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Json(Value),
}

impl From<bool> for SqlValue {
    fn from(value: bool) -> Self {
        SqlValue::Bool(value)
    }
}

impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        SqlValue::Int(value)
    }
}

impl From<i32> for SqlValue {
    fn from(value: i32) -> Self {
        SqlValue::Int(i64::from(value))
    }
}

impl From<u32> for SqlValue {
    fn from(value: u32) -> Self {
        SqlValue::Int(i64::from(value))
    }
}

impl From<f64> for SqlValue {
    fn from(value: f64) -> Self {
        SqlValue::Float(value)
    }
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        SqlValue::Text(value.to_string())
    }
}

impl From<&String> for SqlValue {
    fn from(value: &String) -> Self {
        SqlValue::Text(value.clone())
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        SqlValue::Text(value)
    }
}

impl From<Value> for SqlValue {
    fn from(value: Value) -> Self {
        SqlValue::Json(value)
    }
}

impl<T: Into<SqlValue>> From<Option<T>> for SqlValue {
    fn from(value: Option<T>) -> Self {
        value.map_or(SqlValue::Null, Into::into)
    }
}

/// Builds a `Vec<SqlValue>` from heterogeneous parameters.
#[macro_export]
macro_rules! sql_params {
    ($($value:expr),* $(,)?) => {
        vec![$($crate::sql::SqlValue::from($value)),*]
    };
}

/// One result row, addressed by column name.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlRow {
    columns: Arc<[String]>,
    values: Vec<SqlValue>,
}

fn decode_error(column: &str, expected: &str, value: &SqlValue) -> AntflyError {
    AntflyError::Malformed(format!(
        "column {column}: expected {expected}, got {value:?}"
    ))
}

impl SqlRow {
    pub fn get(&self, column: &str) -> AntflyResult<&SqlValue> {
        self.columns
            .iter()
            .position(|name| name == column)
            .map(|index| &self.values[index])
            .ok_or_else(|| AntflyError::Malformed(format!("missing column {column}")))
    }

    pub fn is_null(&self, column: &str) -> AntflyResult<bool> {
        Ok(matches!(self.get(column)?, SqlValue::Null))
    }

    pub fn opt_i64(&self, column: &str) -> AntflyResult<Option<i64>> {
        match self.get(column)? {
            SqlValue::Null => Ok(None),
            SqlValue::Int(value) => Ok(Some(*value)),
            SqlValue::Text(text) => text
                .parse()
                .map(Some)
                .map_err(|_| decode_error(column, "integer", &SqlValue::Text(text.clone()))),
            other => Err(decode_error(column, "integer", other)),
        }
    }

    pub fn i64(&self, column: &str) -> AntflyResult<i64> {
        self.opt_i64(column)?
            .ok_or_else(|| decode_error(column, "integer", &SqlValue::Null))
    }

    pub fn opt_f64(&self, column: &str) -> AntflyResult<Option<f64>> {
        match self.get(column)? {
            SqlValue::Null => Ok(None),
            SqlValue::Float(value) => Ok(Some(*value)),
            SqlValue::Int(value) => Ok(Some(*value as f64)),
            other => Err(decode_error(column, "number", other)),
        }
    }

    pub fn f64(&self, column: &str) -> AntflyResult<f64> {
        self.opt_f64(column)?
            .ok_or_else(|| decode_error(column, "number", &SqlValue::Null))
    }

    pub fn opt_bool(&self, column: &str) -> AntflyResult<Option<bool>> {
        match self.get(column)? {
            SqlValue::Null => Ok(None),
            SqlValue::Bool(value) => Ok(Some(*value)),
            SqlValue::Int(value) => Ok(Some(*value != 0)),
            other => Err(decode_error(column, "boolean", other)),
        }
    }

    pub fn bool(&self, column: &str) -> AntflyResult<bool> {
        self.opt_bool(column)?
            .ok_or_else(|| decode_error(column, "boolean", &SqlValue::Null))
    }

    pub fn opt_string(&self, column: &str) -> AntflyResult<Option<String>> {
        match self.get(column)? {
            SqlValue::Null => Ok(None),
            SqlValue::Text(text) => Ok(Some(text.clone())),
            other => Err(decode_error(column, "text", other)),
        }
    }

    pub fn string(&self, column: &str) -> AntflyResult<String> {
        self.opt_string(column)?
            .ok_or_else(|| decode_error(column, "text", &SqlValue::Null))
    }

    /// A JSON column, or a text column holding JSON.
    pub fn opt_json(&self, column: &str) -> AntflyResult<Option<Value>> {
        match self.get(column)? {
            SqlValue::Null => Ok(None),
            SqlValue::Json(value) => Ok(Some(value.clone())),
            SqlValue::Text(text) => serde_json::from_str(text).map(Some).map_err(Into::into),
            other => Err(decode_error(column, "json", other)),
        }
    }

    pub fn json(&self, column: &str) -> AntflyResult<Value> {
        self.opt_json(column)?
            .ok_or_else(|| decode_error(column, "json", &SqlValue::Null))
    }
}

fn sql_error(error: sqlx::Error) -> AntflyError {
    match error {
        sqlx::Error::Database(database) => AntflyError::Sql {
            code: database.code().map(std::borrow::Cow::into_owned),
            message: database.message().to_string(),
        },
        sqlx::Error::RowNotFound => AntflyError::Sql {
            code: None,
            message: "no rows returned".to_string(),
        },
        other => AntflyError::Sql {
            code: None,
            message: other.to_string(),
        },
    }
}

fn antfly_arguments(params: Vec<SqlValue>) -> AntflyResult<AntflyArguments> {
    let mut arguments = AntflyArguments::default();
    for value in params {
        let added = match value {
            SqlValue::Null => arguments.add(Option::<Value>::None),
            SqlValue::Bool(value) => arguments.add(value),
            SqlValue::Int(value) => arguments.add(value),
            SqlValue::Float(value) => arguments.add(value),
            SqlValue::Text(value) => arguments.add(value),
            SqlValue::Json(value) => arguments.add(value),
        };
        added.map_err(|err| AntflyError::Malformed(err.to_string()))?;
    }
    Ok(arguments)
}

fn pg_arguments(params: Vec<SqlValue>) -> AntflyResult<PgArguments> {
    let mut arguments = PgArguments::default();
    for value in params {
        let added = match value {
            SqlValue::Null => arguments.add(Option::<String>::None),
            SqlValue::Bool(value) => arguments.add(value),
            SqlValue::Int(value) => arguments.add(value),
            SqlValue::Float(value) => arguments.add(value),
            SqlValue::Text(value) => arguments.add(value),
            SqlValue::Json(value) => arguments.add(sqlx::types::Json(value)),
        };
        added.map_err(|err| AntflyError::Malformed(err.to_string()))?;
    }
    Ok(arguments)
}

fn decode_cell<'r, DB, R>(row: &'r R, index: usize) -> AntflyResult<SqlValue>
where
    DB: sqlx::Database,
    R: Row<Database = DB>,
    usize: sqlx::ColumnIndex<R>,
    i64: sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    f64: sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    bool: sqlx::Decode<'r, DB> + sqlx::Type<DB>,
    String: sqlx::Decode<'r, DB> + sqlx::Type<DB>,
{
    let raw = row.try_get_raw(index).map_err(sql_error)?;
    if raw.is_null() {
        return Ok(SqlValue::Null);
    }
    let type_name = raw.type_info().name().to_ascii_lowercase();
    let cell = match type_name.as_str() {
        "integer" | "int8" | "int4" | "int2" | "bigint" | "int" | "smallint" => {
            SqlValue::Int(row.try_get::<i64, _>(index).map_err(sql_error)?)
        }
        "number" | "float8" | "float4" | "double precision" | "real" => {
            SqlValue::Float(row.try_get::<f64, _>(index).map_err(sql_error)?)
        }
        "boolean" | "bool" => SqlValue::Bool(row.try_get::<bool, _>(index).map_err(sql_error)?),
        "json" | "jsonb" => {
            let text = row.try_get::<String, _>(index).map_err(sql_error);
            match text {
                Ok(text) => SqlValue::Json(serde_json::from_str(&text)?),
                Err(err) => return Err(err),
            }
        }
        _ => SqlValue::Text(row.try_get::<String, _>(index).map_err(sql_error)?),
    };
    Ok(cell)
}

fn antfly_row(row: &antfly_embedded::sqlx::AntflyRow) -> AntflyResult<SqlRow> {
    let columns: Arc<[String]> = row
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let mut values = Vec::with_capacity(columns.len());
    for index in 0..columns.len() {
        let raw = row.try_get_raw(index).map_err(sql_error)?;
        if raw.is_null() {
            values.push(SqlValue::Null);
            continue;
        }
        let type_name = raw.type_info().name().to_ascii_lowercase();
        values.push(if matches!(type_name.as_str(), "json" | "jsonb") {
            SqlValue::Json(row.try_get::<Value, _>(index).map_err(sql_error)?)
        } else {
            decode_cell(row, index)?
        });
    }
    Ok(SqlRow { columns, values })
}

fn pg_row(row: &sqlx_postgres::PgRow) -> AntflyResult<SqlRow> {
    let columns: Arc<[String]> = row
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect();
    let mut values = Vec::with_capacity(columns.len());
    for index in 0..columns.len() {
        let raw = row.try_get_raw(index).map_err(sql_error)?;
        if raw.is_null() {
            values.push(SqlValue::Null);
            continue;
        }
        let type_name = raw.type_info().name().to_ascii_lowercase();
        values.push(if matches!(type_name.as_str(), "json" | "jsonb") {
            SqlValue::Json(
                row.try_get::<sqlx::types::Json<Value>, _>(index)
                    .map_err(sql_error)?
                    .0,
            )
        } else {
            decode_cell(row, index)?
        });
    }
    Ok(SqlRow { columns, values })
}

#[derive(Clone, Debug)]
enum Pool {
    Embedded(sqlx::Pool<AntflyDb>),
    Remote(sqlx::Pool<Postgres>),
}

/// A pool of SQL connections to the Codex database.
#[derive(Clone, Debug)]
pub struct Sql {
    pool: Pool,
}

impl Sql {
    /// Opens connections on an embedded `.aflite` file. libantfly queues
    /// writers fairly, so this can share the file with a `Database` handle.
    #[expect(
        clippy::disallowed_methods,
        reason = "the SQLite pool constructors are banned in favor of codex-state's shim; this pool is Antfly's, not SQLite's"
    )]
    pub async fn connect_embedded(
        path: impl Into<std::path::PathBuf>,
        no_sync: bool,
    ) -> AntflyResult<Self> {
        let options = AntflyConnectOptions::new(path).no_sync(no_sync);
        let pool = sqlx::pool::PoolOptions::<AntflyDb>::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(sql_error)?;
        Ok(Self {
            pool: Pool::Embedded(pool),
        })
    }

    /// Opens connections to a remote Antfly PostgreSQL wire listener, e.g.
    /// `postgres://user:password@127.0.0.1:5432/antfly`.
    #[expect(
        clippy::disallowed_methods,
        reason = "the SQLite pool constructors are banned in favor of codex-state's shim; this pool is PostgreSQL's, not SQLite's"
    )]
    pub async fn connect_remote(url: &str) -> AntflyResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await
            .map_err(sql_error)?;
        Ok(Self {
            pool: Pool::Remote(pool),
        })
    }

    pub fn execute<'a>(
        &'a self,
        statement: &'a str,
        params: Vec<SqlValue>,
    ) -> BoxFuture<'a, AntflyResult<u64>> {
        Box::pin(async move {
            let statement = AssertSqlSafe(statement.to_string());
            match &self.pool {
                Pool::Embedded(pool) => sqlx::query_with(statement, antfly_arguments(params)?)
                    .execute(pool)
                    .await
                    .map(|result| result.rows_affected())
                    .map_err(sql_error),
                Pool::Remote(pool) => sqlx::query_with(statement, pg_arguments(params)?)
                    .execute(pool)
                    .await
                    .map(|result| result.rows_affected())
                    .map_err(sql_error),
            }
        })
    }

    pub fn fetch_all<'a>(
        &'a self,
        statement: &'a str,
        params: Vec<SqlValue>,
    ) -> BoxFuture<'a, AntflyResult<Vec<SqlRow>>> {
        Box::pin(async move {
            let statement = AssertSqlSafe(statement.to_string());
            match &self.pool {
                Pool::Embedded(pool) => sqlx::query_with(statement, antfly_arguments(params)?)
                    .fetch_all(pool)
                    .await
                    .map_err(sql_error)?
                    .iter()
                    .map(antfly_row)
                    .collect(),
                Pool::Remote(pool) => sqlx::query_with(statement, pg_arguments(params)?)
                    .fetch_all(pool)
                    .await
                    .map_err(sql_error)?
                    .iter()
                    .map(pg_row)
                    .collect(),
            }
        })
    }

    pub fn fetch_optional<'a>(
        &'a self,
        statement: &'a str,
        params: Vec<SqlValue>,
    ) -> BoxFuture<'a, AntflyResult<Option<SqlRow>>> {
        Box::pin(async move { Ok(self.fetch_all(statement, params).await?.into_iter().next()) })
    }

    /// Starts a READ COMMITTED transaction. Concurrent commits to the same
    /// rows fail with SQLSTATE 40001 ([`AntflyError::is_conflict`]).
    pub fn begin(&self) -> BoxFuture<'_, AntflyResult<SqlTx>> {
        Box::pin(async move {
            Ok(SqlTx {
                inner: match &self.pool {
                    Pool::Embedded(pool) => Tx::Embedded(pool.begin().await.map_err(sql_error)?),
                    Pool::Remote(pool) => Tx::Remote(pool.begin().await.map_err(sql_error)?),
                },
            })
        })
    }

    pub async fn close(&self) {
        match &self.pool {
            Pool::Embedded(pool) => pool.close().await,
            Pool::Remote(pool) => pool.close().await,
        }
    }
}

enum Tx {
    Embedded(sqlx::Transaction<'static, AntflyDb>),
    Remote(sqlx::Transaction<'static, Postgres>),
}

/// An open transaction; dropped without [`SqlTx::commit`] it rolls back.
pub struct SqlTx {
    inner: Tx,
}

impl SqlTx {
    pub fn execute<'a>(
        &'a mut self,
        statement: &'a str,
        params: Vec<SqlValue>,
    ) -> BoxFuture<'a, AntflyResult<u64>> {
        Box::pin(async move {
            let statement = AssertSqlSafe(statement.to_string());
            match &mut self.inner {
                Tx::Embedded(tx) => sqlx::query_with(statement, antfly_arguments(params)?)
                    .execute(&mut **tx)
                    .await
                    .map(|result| result.rows_affected())
                    .map_err(sql_error),
                Tx::Remote(tx) => sqlx::query_with(statement, pg_arguments(params)?)
                    .execute(&mut **tx)
                    .await
                    .map(|result| result.rows_affected())
                    .map_err(sql_error),
            }
        })
    }

    pub fn fetch_all<'a>(
        &'a mut self,
        statement: &'a str,
        params: Vec<SqlValue>,
    ) -> BoxFuture<'a, AntflyResult<Vec<SqlRow>>> {
        Box::pin(async move {
            let statement = AssertSqlSafe(statement.to_string());
            match &mut self.inner {
                Tx::Embedded(tx) => sqlx::query_with(statement, antfly_arguments(params)?)
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(sql_error)?
                    .iter()
                    .map(antfly_row)
                    .collect(),
                Tx::Remote(tx) => sqlx::query_with(statement, pg_arguments(params)?)
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(sql_error)?
                    .iter()
                    .map(pg_row)
                    .collect(),
            }
        })
    }

    pub fn fetch_optional<'a>(
        &'a mut self,
        statement: &'a str,
        params: Vec<SqlValue>,
    ) -> BoxFuture<'a, AntflyResult<Option<SqlRow>>> {
        Box::pin(async move { Ok(self.fetch_all(statement, params).await?.into_iter().next()) })
    }

    pub fn commit(self) -> BoxFuture<'static, AntflyResult<()>> {
        Box::pin(async move {
            match self.inner {
                Tx::Embedded(tx) => tx.commit().await.map_err(sql_error),
                Tx::Remote(tx) => tx.commit().await.map_err(sql_error),
            }
        })
    }

    pub fn rollback(self) -> BoxFuture<'static, AntflyResult<()>> {
        Box::pin(async move {
            match self.inner {
                Tx::Embedded(tx) => tx.rollback().await.map_err(sql_error),
                Tx::Remote(tx) => tx.rollback().await.map_err(sql_error),
            }
        })
    }
}
