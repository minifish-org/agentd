use super::*;

pub type Error = StoreError;

fn decode(message: String) -> Error {
    Error::database(anyhow!(message))
}

#[derive(Clone)]
pub struct LibsqlPool {
    db: Arc<Database>,
    conn: Arc<tokio::sync::Mutex<Connection>>,
}

impl LibsqlPool {
    pub async fn open(path: &str) -> std::result::Result<Self, Error> {
        if path != ":memory:" {
            if let Some(parent) = Path::new(path)
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
            {
                tokio::fs::create_dir_all(parent)
                    .await
                    .map_err(|err| decode(err.to_string()))?;
            }
        }
        let db = Builder::new_local(path).build().await?;
        let conn = db.connect()?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA busy_timeout=5000;
            PRAGMA foreign_keys=ON;
            "#,
        )
        .await?;
        let pool = Self {
            db: Arc::new(db),
            conn: Arc::new(tokio::sync::Mutex::new(conn)),
        };
        Ok(pool)
    }

    pub fn conn(&self) -> std::result::Result<Connection, Error> {
        let conn = self.db.connect()?;
        conn.busy_timeout(Duration::from_secs(5))?;
        Ok(conn)
    }

    pub async fn begin(&self) -> std::result::Result<Transaction, Error> {
        Ok(Transaction {
            inner: self.conn()?.transaction().await?,
        })
    }

    pub async fn begin_immediate(&self) -> std::result::Result<Transaction, Error> {
        Ok(Transaction {
            inner: self
                .conn()?
                .transaction_with_behavior(libsql::TransactionBehavior::Immediate)
                .await?,
        })
    }
}

pub struct Transaction {
    inner: libsql::Transaction,
}

impl Transaction {
    pub async fn commit(self) -> std::result::Result<(), Error> {
        Ok(self.inner.commit().await?)
    }

    pub async fn rollback(self) -> std::result::Result<(), Error> {
        Ok(self.inner.rollback().await?)
    }
}

pub fn query(sql: &str) -> Query {
    Query {
        sql: sql.to_string(),
        params: Vec::new(),
    }
}

pub fn query_scalar<T>(sql: &str) -> QueryScalar<T> {
    QueryScalar {
        query: query(sql),
        _marker: std::marker::PhantomData,
    }
}

pub struct Query {
    sql: String,
    params: Vec<Value>,
}

impl Query {
    pub fn bind<T: IntoSqlValue>(mut self, value: T) -> Self {
        self.params.push(value.into_sql_value());
        self
    }

    pub async fn execute<E: Executor>(
        self,
        executor: E,
    ) -> std::result::Result<ExecuteResult, Error> {
        executor.execute(&self.sql, self.params).await
    }

    pub async fn fetch_all<E: Executor>(
        self,
        executor: E,
    ) -> std::result::Result<Vec<SqlRow>, Error> {
        executor.fetch_all(&self.sql, self.params).await
    }

    pub async fn fetch_optional<E: Executor>(
        self,
        executor: E,
    ) -> std::result::Result<Option<SqlRow>, Error> {
        let rows = self.fetch_all(executor).await?;
        Ok(rows.into_iter().next())
    }
}

pub struct ExecuteResult {
    rows_affected: u64,
}

impl ExecuteResult {
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }
}

pub struct QueryScalar<T> {
    query: Query,
    _marker: std::marker::PhantomData<T>,
}

impl<T> QueryScalar<T> {
    pub fn bind<U: IntoSqlValue>(mut self, value: U) -> Self {
        self.query = self.query.bind(value);
        self
    }
}

impl<T: FromSqlValue> QueryScalar<T> {
    pub async fn fetch_all<E: Executor>(self, executor: E) -> std::result::Result<Vec<T>, Error> {
        let rows = self.query.fetch_all(executor).await?;
        rows.into_iter().map(|row| row.try_get(0)).collect()
    }

    pub async fn fetch_optional<E: Executor>(
        self,
        executor: E,
    ) -> std::result::Result<Option<T>, Error> {
        let row = self.query.fetch_optional(executor).await?;
        row.map(|row| row.try_get(0)).transpose()
    }
}

pub trait Executor {
    fn conn(&self) -> std::result::Result<CowConnection<'_>, Error>;

    async fn execute(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> std::result::Result<ExecuteResult, Error> {
        let rows_affected = match self.conn()? {
            CowConnection::Connection(conn) => {
                conn.execute(sql, libsql::params_from_iter(params)).await?
            }
            CowConnection::Transaction(tx) => {
                tx.inner
                    .execute(sql, libsql::params_from_iter(params))
                    .await?
            }
        };
        Ok(ExecuteResult { rows_affected })
    }

    async fn fetch_all(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> std::result::Result<Vec<SqlRow>, Error> {
        let mut rows = match self.conn()? {
            CowConnection::Connection(conn) => {
                conn.query(sql, libsql::params_from_iter(params)).await?
            }
            CowConnection::Transaction(tx) => {
                tx.inner
                    .query(sql, libsql::params_from_iter(params))
                    .await?
            }
        };
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(SqlRow::from_libsql_row(row)?);
        }
        Ok(out)
    }
}

pub enum CowConnection<'a> {
    Connection(Connection),
    Transaction(&'a Transaction),
}

impl Executor for &LibsqlPool {
    fn conn(&self) -> std::result::Result<CowConnection<'_>, Error> {
        Ok(CowConnection::Connection(LibsqlPool::conn(self)?))
    }

    async fn execute(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> std::result::Result<ExecuteResult, Error> {
        let conn = self.conn.lock().await;
        let rows_affected = conn.execute(sql, libsql::params_from_iter(params)).await?;
        Ok(ExecuteResult { rows_affected })
    }

    async fn fetch_all(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> std::result::Result<Vec<SqlRow>, Error> {
        let conn = self.conn.lock().await;
        let mut rows = conn.query(sql, libsql::params_from_iter(params)).await?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(SqlRow::from_libsql_row(row)?);
        }
        Ok(out)
    }
}

impl Executor for &mut Transaction {
    fn conn(&self) -> std::result::Result<CowConnection<'_>, Error> {
        Ok(CowConnection::Transaction(self))
    }
}

pub struct SqlRow {
    values: Vec<Value>,
    names: Vec<String>,
}

impl SqlRow {
    fn from_libsql_row(row: libsql::Row) -> std::result::Result<Self, Error> {
        let mut values = Vec::new();
        let mut names = Vec::new();
        for idx in 0..row.column_count() {
            values.push(row.get_value(idx)?);
            names.push(row.column_name(idx).unwrap_or("").to_string());
        }
        Ok(Self { values, names })
    }
}

pub trait Row {
    fn try_get<T, I>(&self, index: I) -> std::result::Result<T, Error>
    where
        T: FromSqlValue,
        I: RowIndex;
}

impl Row for SqlRow {
    fn try_get<T, I>(&self, index: I) -> std::result::Result<T, Error>
    where
        T: FromSqlValue,
        I: RowIndex,
    {
        let idx = index.index(self)?;
        let value = self
            .values
            .get(idx as usize)
            .cloned()
            .ok_or_else(|| decode(format!("column not found: {idx}")))?;
        T::from_sql_value(value)
    }
}

pub trait RowIndex {
    fn index(self, row: &SqlRow) -> std::result::Result<i32, Error>;
}

impl RowIndex for i32 {
    fn index(self, _row: &SqlRow) -> std::result::Result<i32, Error> {
        Ok(self)
    }
}

impl RowIndex for usize {
    fn index(self, _row: &SqlRow) -> std::result::Result<i32, Error> {
        Ok(self as i32)
    }
}

impl RowIndex for &str {
    fn index(self, row: &SqlRow) -> std::result::Result<i32, Error> {
        for (idx, name) in row.names.iter().enumerate() {
            if name == self {
                return Ok(idx as i32);
            }
        }
        Err(decode(format!("column not found: {self}")))
    }
}

pub trait IntoSqlValue {
    fn into_sql_value(self) -> Value;
}

impl IntoSqlValue for Value {
    fn into_sql_value(self) -> Value {
        self
    }
}

impl IntoSqlValue for &str {
    fn into_sql_value(self) -> Value {
        Value::Text(self.to_string())
    }
}

impl IntoSqlValue for String {
    fn into_sql_value(self) -> Value {
        Value::Text(self)
    }
}

impl IntoSqlValue for &String {
    fn into_sql_value(self) -> Value {
        Value::Text(self.clone())
    }
}

impl IntoSqlValue for Option<&str> {
    fn into_sql_value(self) -> Value {
        self.map_or(Value::Null, |value| Value::Text(value.to_string()))
    }
}

impl IntoSqlValue for Option<String> {
    fn into_sql_value(self) -> Value {
        self.map_or(Value::Null, Value::Text)
    }
}

impl IntoSqlValue for &Option<String> {
    fn into_sql_value(self) -> Value {
        self.clone().into_sql_value()
    }
}

impl IntoSqlValue for i64 {
    fn into_sql_value(self) -> Value {
        Value::Integer(self)
    }
}

impl IntoSqlValue for Option<i64> {
    fn into_sql_value(self) -> Value {
        self.map_or(Value::Null, Value::Integer)
    }
}

impl IntoSqlValue for i32 {
    fn into_sql_value(self) -> Value {
        Value::Integer(self as i64)
    }
}

impl IntoSqlValue for u64 {
    fn into_sql_value(self) -> Value {
        Value::Integer(self as i64)
    }
}

impl IntoSqlValue for bool {
    fn into_sql_value(self) -> Value {
        Value::Integer(if self { 1 } else { 0 })
    }
}

impl IntoSqlValue for f64 {
    fn into_sql_value(self) -> Value {
        Value::Real(self)
    }
}

impl IntoSqlValue for Option<f64> {
    fn into_sql_value(self) -> Value {
        self.map_or(Value::Null, Value::Real)
    }
}

impl IntoSqlValue for f32 {
    fn into_sql_value(self) -> Value {
        Value::Real(self as f64)
    }
}

impl IntoSqlValue for &[u8] {
    fn into_sql_value(self) -> Value {
        Value::Blob(self.to_vec())
    }
}

impl IntoSqlValue for Vec<u8> {
    fn into_sql_value(self) -> Value {
        Value::Blob(self)
    }
}

impl IntoSqlValue for &Vec<u8> {
    fn into_sql_value(self) -> Value {
        Value::Blob(self.clone())
    }
}

pub trait FromSqlValue: Sized {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error>;
}

impl FromSqlValue for String {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Text(value) => Ok(value),
            Value::Integer(value) => Ok(value.to_string()),
            Value::Real(value) => Ok(value.to_string()),
            Value::Blob(value) => {
                String::from_utf8(value).map_err(|error| decode(error.to_string()))
            }
            Value::Null => Err(decode("expected TEXT, found NULL".into())),
        }
    }
}

impl FromSqlValue for Option<String> {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            other => Ok(Some(String::from_sql_value(other)?)),
        }
    }
}

impl FromSqlValue for i64 {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Integer(value) => Ok(value),
            other => Err(decode(format!("expected INTEGER, found {other:?}"))),
        }
    }
}

impl FromSqlValue for Option<i64> {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            Value::Integer(value) => Ok(Some(value)),
            other => Err(decode(format!("expected optional integer, got {other:?}"))),
        }
    }
}

impl FromSqlValue for i32 {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        Ok(i64::from_sql_value(value)? as i32)
    }
}

impl FromSqlValue for u64 {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        Ok(i64::from_sql_value(value)? as u64)
    }
}

impl FromSqlValue for f64 {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Real(value) => Ok(value),
            Value::Integer(value) => Ok(value as f64),
            other => Err(decode(format!("expected REAL, found {other:?}"))),
        }
    }
}

impl FromSqlValue for Option<f64> {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            other => Ok(Some(f64::from_sql_value(other)?)),
        }
    }
}

impl FromSqlValue for bool {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        Ok(i64::from_sql_value(value)? != 0)
    }
}

impl FromSqlValue for Vec<u8> {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Blob(value) => Ok(value),
            other => Err(decode(format!("expected BLOB, found {other:?}"))),
        }
    }
}

impl FromSqlValue for Option<Vec<u8>> {
    fn from_sql_value(value: Value) -> std::result::Result<Self, Error> {
        match value {
            Value::Null => Ok(None),
            other => Ok(Some(Vec::<u8>::from_sql_value(other)?)),
        }
    }
}
