//! Query execution on one connection — the lib/pq `simpleQuery` /
//! `simpleExec` / `stmt.exec` / `rows.Next` state machine.
//!
//! A query is started with [`Conn::start_query`]; rows are then pulled with
//! [`Conn::next_row`] until it returns `Ok(None)` (the `ReadyForQuery` of the
//! statement was consumed). [`Conn::finish`] drains whatever is left so the
//! connection can be reused.

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;

use super::value::{decode_text, Column, DriverValue};
use super::wire::{parse_command_tag, parse_row_description, Conn, ExecResult};
use super::{PgError, SqlArg};

/// State right after a statement was sent.
#[derive(Debug)]
pub struct QueryStart {
    /// Result columns (empty for statements that return no rows).
    pub columns: Vec<Column>,
    /// The statement already completed and `ReadyForQuery` was consumed —
    /// no rows will follow (lib/pq `rows.done`).
    pub done: bool,
    /// Command completion seen so far (lib/pq `rows.result`).
    pub result: Option<ExecResult>,
}

fn unexpected_ready() -> PgError {
    PgError::Other("unexpected ReadyForQuery".to_string())
}

impl Conn {
    /// Send `query` and read up to the first data row (simple protocol when
    /// there are no arguments, extended protocol otherwise — exactly the
    /// split lib/pq makes). Statement errors are returned here, with the
    /// connection re-synchronised (`ReadyForQuery` consumed).
    pub fn start_query(&mut self, query: &str, args: &[SqlArg]) -> Result<QueryStart, PgError> {
        if self.broken {
            return Err(PgError::BadConn);
        }
        if self.in_flight {
            // Leftovers of a previous, not fully read result set.
            self.finish()?;
        }
        if !args.is_empty() {
            let columns = self.send_extended_query(query, args)?;
            self.in_flight = true;
            return Ok(QueryStart {
                columns,
                done: false,
                result: None,
            });
        }
        self.send_simple_query(query)?;
        self.in_flight = true;
        let mut columns: Vec<Column> = Vec::new();
        let mut have_description = false;
        let mut result: Option<ExecResult> = None;
        let mut err: Option<PgError> = None;
        loop {
            let m = match self.recv() {
                Ok(m) => m,
                Err(e) => {
                    self.in_flight = false;
                    return Err(e);
                }
            };
            match m {
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| PgError::pq(e.to_string()))?;
                    result = Some(ExecResult::RowsAffected(parse_command_tag(tag)?));
                    if have_description {
                        // Empty result set with a row description: rows.Next
                        // will consume the ReadyForQuery.
                        return Ok(QueryStart {
                            columns,
                            done: false,
                            result,
                        });
                    }
                }
                Message::EmptyQueryResponse => {
                    result = Some(ExecResult::EmptyQuery);
                }
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    self.in_flight = false;
                    return match err {
                        Some(e) => Err(e),
                        None => Ok(QueryStart {
                            columns,
                            done: true,
                            result,
                        }),
                    };
                }
                Message::ErrorResponse(body) => {
                    let e = self.server_error(body.fields())?;
                    if self.broken {
                        self.in_flight = false;
                        return Err(e);
                    }
                    err = Some(e);
                }
                Message::DataRow(_) => {
                    if !have_description {
                        self.broken = true;
                        self.in_flight = false;
                        return Err(PgError::pq(
                            "unexpected DataRow in simple query execution".to_string(),
                        ));
                    }
                    // Kick off to next_row.
                    self.saved = Some(m);
                    return Ok(QueryStart {
                        columns,
                        done: false,
                        result,
                    });
                }
                Message::RowDescription(body) => {
                    columns = parse_row_description(body.fields())?;
                    have_description = true;
                }
                _ => {
                    self.broken = true;
                    self.in_flight = false;
                    return Err(PgError::pq("unknown response for simple query".to_string()));
                }
            }
        }
    }

    /// Read the next data row of the running statement. `Ok(None)` when the
    /// statement is finished (its `ReadyForQuery` was consumed) or when the
    /// next result set of a multi-statement query begins (lib/pq returns
    /// `io.EOF` there and DevStats never reads further result sets).
    pub fn next_row(
        &mut self,
        columns: &[Column],
        result: &mut Option<ExecResult>,
    ) -> Result<Option<Vec<DriverValue>>, PgError> {
        if !self.in_flight {
            return Ok(None);
        }
        let mut err: Option<PgError> = None;
        loop {
            let m = match self.recv() {
                Ok(m) => m,
                Err(e) => {
                    self.in_flight = false;
                    return Err(e);
                }
            };
            match m {
                Message::ErrorResponse(body) => {
                    let e = self.server_error(body.fields())?;
                    if self.broken {
                        self.in_flight = false;
                        return Err(e);
                    }
                    err = Some(e);
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| PgError::pq(e.to_string()))?;
                    *result = Some(ExecResult::RowsAffected(parse_command_tag(tag)?));
                }
                Message::EmptyQueryResponse => {
                    *result = Some(ExecResult::EmptyQuery);
                }
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    self.in_flight = false;
                    return match err {
                        Some(e) => Err(e),
                        None => Ok(None),
                    };
                }
                Message::DataRow(body) => {
                    if let Some(e) = err {
                        self.broken = true;
                        self.in_flight = false;
                        return Err(PgError::pq(format!("unexpected DataRow after error {}", e)));
                    }
                    let buf = body.buffer();
                    let mut ranges = body.ranges();
                    let mut values: Vec<DriverValue> = Vec::with_capacity(columns.len());
                    while let Some(range) = ranges.next().map_err(|e| PgError::pq(e.to_string()))? {
                        let Some(col) = columns.get(values.len()) else {
                            break;
                        };
                        match range {
                            None => values.push(DriverValue::Null),
                            Some(r) => {
                                let v = decode_text(&buf[r], col.type_oid).map_err(PgError::pq)?;
                                values.push(v);
                            }
                        }
                    }
                    while values.len() < columns.len() {
                        values.push(DriverValue::Null);
                    }
                    return Ok(Some(values));
                }
                Message::RowDescription(body) => {
                    // Next result set of a multi-statement query.
                    let _ = parse_row_description(body.fields())?;
                    return Ok(None);
                }
                Message::PortalSuspended
                | Message::NoData
                | Message::ParseComplete
                | Message::BindComplete => {}
                _ => {
                    self.broken = true;
                    self.in_flight = false;
                    return Err(PgError::pq("unexpected message after execute".to_string()));
                }
            }
        }
    }

    /// Consume everything up to the `ReadyForQuery` of the running statement
    /// (lib/pq `rows.Close`), recording command completions in `result` and
    /// returning the first error seen.
    pub fn finish_with_result(&mut self, result: &mut Option<ExecResult>) -> Result<(), PgError> {
        if !self.in_flight {
            return Ok(());
        }
        let mut first_err: Option<PgError> = None;
        loop {
            let m = match self.recv() {
                Ok(m) => m,
                Err(e) => {
                    self.in_flight = false;
                    return Err(e);
                }
            };
            match m {
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    self.in_flight = false;
                    return match first_err {
                        Some(e) => Err(e),
                        None => Ok(()),
                    };
                }
                Message::ErrorResponse(body) => {
                    let e = self.server_error(body.fields())?;
                    if self.broken {
                        self.in_flight = false;
                        return Err(e);
                    }
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Message::CommandComplete(body) => {
                    let tag = body.tag().map_err(|e| PgError::pq(e.to_string()))?;
                    match parse_command_tag(tag) {
                        Ok(n) => *result = Some(ExecResult::RowsAffected(n)),
                        Err(e) => {
                            if first_err.is_none() {
                                first_err = Some(e);
                            }
                        }
                    }
                }
                Message::EmptyQueryResponse => *result = Some(ExecResult::EmptyQuery),
                _ => {}
            }
        }
    }

    /// [`finish_with_result`](Self::finish_with_result) without caring about
    /// the command result.
    pub fn finish(&mut self) -> Result<(), PgError> {
        let mut result = None;
        self.finish_with_result(&mut result)
    }

    /// Execute a statement and return its result (Go `driver.Execer`).
    pub fn exec(&mut self, query: &str, args: &[SqlArg]) -> Result<ExecResult, PgError> {
        let start = self.start_query(query, args)?;
        let mut result = start.result;
        if !start.done {
            self.finish_with_result(&mut result)?;
        }
        result.ok_or_else(unexpected_ready)
    }

    /// Simple-protocol statement returning its command tag (lib/pq
    /// `simpleExec`, used for `BEGIN`/`COMMIT`/`ROLLBACK`).
    pub fn simple_exec_tag(&mut self, query: &str) -> Result<String, PgError> {
        if self.broken {
            return Err(PgError::BadConn);
        }
        if self.in_flight {
            self.finish()?;
        }
        self.send_simple_query(query)?;
        self.in_flight = true;
        let mut tag: Option<String> = None;
        let mut err: Option<PgError> = None;
        loop {
            let m = match self.recv() {
                Ok(m) => m,
                Err(e) => {
                    self.in_flight = false;
                    return Err(e);
                }
            };
            match m {
                Message::CommandComplete(body) => {
                    tag = Some(
                        body.tag()
                            .map_err(|e| PgError::pq(e.to_string()))?
                            .to_string(),
                    );
                }
                Message::ReadyForQuery(body) => {
                    self.set_txn_status(body.status());
                    self.in_flight = false;
                    return match (err, tag) {
                        (Some(e), _) => Err(e),
                        (None, Some(t)) => Ok(t),
                        (None, None) => Err(unexpected_ready()),
                    };
                }
                Message::ErrorResponse(body) => {
                    let e = self.server_error(body.fields())?;
                    if self.broken {
                        self.in_flight = false;
                        return Err(e);
                    }
                    err = Some(e);
                }
                Message::EmptyQueryResponse => tag = Some(String::new()),
                Message::RowDescription(_) | Message::DataRow(_) => {}
                _ => {
                    self.broken = true;
                    self.in_flight = false;
                    return Err(PgError::pq("unknown response for simple query".to_string()));
                }
            }
        }
    }
}
