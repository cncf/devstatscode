//! `runq` — Rust port of `cmd/runq/runq.go`.
//!
//! Runs an SQL file against the project's PostgreSQL database after applying
//! `{{param}} value` replacements from the command line and prints the result
//! as an ASCII table (optionally also as CSV, `GHA2DB_CSVOUT`). Environment
//! (`GHA2DB_LOCAL`, `GHA2DB_ABSOLUTE`, `GHA2DB_DATADIR`, `GHA2DB_EXPLAIN`,
//! `GHA2DB_DRY_RUN`, `GHA2DB_DEBUG`, `PG_*`), arguments, output and exit codes
//! are those of the Go program.

use std::collections::BTreeMap;
use std::fs::File;
use std::process;
use std::time::Instant;

use devstatscode::error::go_io_error_string;
use devstatscode::pg::ScanDest;
use devstatscode::{
    fatal_on_error, fatalf, gocsv, io, pg, printf, printf_bytes, signal, string as gostring,
    time as gotime, Ctx,
};

/// Go `utf8.RuneCountInString`: every byte of an invalid sequence counts as
/// one rune (`fmt`'s `%-Ns` pads by this count while the column widths are
/// byte lengths).
fn rune_count(s: &[u8]) -> usize {
    s.utf8_chunks()
        .map(|c| c.valid().chars().count() + c.invalid().len())
        .sum()
}

/// Go `fmt.Sprintf("%-<width>s", value)` appended to `out`.
fn pad_left_aligned(out: &mut Vec<u8>, value: &[u8], width: usize) {
    out.extend_from_slice(value);
    let n = rune_count(value);
    if width > n {
        out.extend(std::iter::repeat_n(b' ', width - n));
    }
}

/// One frame line: `<left>` + `---+---` + `<right>\n`.
fn frame(widths: &[usize], left: u8, right: u8) -> Vec<u8> {
    let mut out = vec![left];
    for &w in widths {
        out.extend(std::iter::repeat_n(b'-', w));
        out.push(b'+');
    }
    out.pop();
    out.push(right);
    out.push(b'\n');
    out
}

/// One table row: `|v1|v2|\n` with every value padded to its column width.
fn row_line(values: &[Vec<u8>], widths: &[usize]) -> Vec<u8> {
    let mut out = vec![b'|'];
    for (value, &w) in values.iter().zip(widths) {
        pad_left_aligned(&mut out, value, w);
        out.push(b'|');
    }
    out.push(b'\n');
    out
}

/// Go `runq`: returns the context (for the `Time:` line decision).
fn runq(sql_file: &str, params: &[String]) -> Ctx {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // SQL arguments number
    if !params.len().is_multiple_of(2) {
        printf!(
            "Must provide correct parameter value pairs: [{}]\n",
            params.join(" ")
        );
        process::exit(1);
    }

    // SQL arguments parse
    let mut replaces: BTreeMap<String, String> = BTreeMap::new();
    let mut param_name = String::new();
    for (index, param) in params.iter().enumerate() {
        if index % 2 == 0 {
            replaces.insert(param.clone(), String::new());
            param_name = param.clone();
        } else {
            let mut value = param.clone();
            // Support special "readfile:replacement.dat" mode
            if param.len() >= 10 && param.starts_with("readfile:") {
                let fn_ = &param[9..];
                if ctx.debug > 0 {
                    printf!("Reading file: {}\n", fn_);
                }
                let bytes = match io::read_file(&ctx, fn_) {
                    Ok(b) => b,
                    Err(e) => fatal_on_error(e),
                };
                value = String::from_utf8_lossy(&bytes).into_owned();
            }
            replaces.insert(std::mem::take(&mut param_name), value);
        }
    }

    // Local or cron mode?
    let mut data_prefix = ctx.data_dir.clone();
    if ctx.local {
        data_prefix = "./".to_string();
    }
    // Absolute mode only allowed in 'runq' tool.
    if ctx.absolute {
        data_prefix = String::new();
    }

    // Read and eventually transform SQL file.
    let bytes = match io::read_file(&ctx, &format!("{data_prefix}{sql_file}")) {
        Ok(b) => b,
        Err(e) => fatal_on_error(e),
    };
    let mut sql_query = String::from_utf8_lossy(&bytes).into_owned();
    let mut qr_period = "";
    let mut qr_from = "";
    let mut qr_to = "";
    let mut qr = false;
    for (from, to) in &replaces {
        // Special replace 'qr' 'period,from,to' is used for {{period.alias.name}} replacements
        if from == "qr" {
            let qr_ary: Vec<&str> = to.split(',').collect();
            if qr_ary.len() < 3 {
                fatalf!("qr parameter must be 'period,from,to', got: '{}'", to);
            }
            qr = true;
            (qr_period, qr_from, qr_to) = (qr_ary[0], qr_ary[1], qr_ary[2]);
            continue;
        }
        // Like Go's `strings.Replace(…, -1)`, an empty `from` inserts `to` at
        // every character boundary.
        sql_query = sql_query.replace(from.as_str(), to);
    }
    if qr {
        let (query, s_hours) =
            gostring::prepare_quick_range_query(&sql_query, qr_period, qr_from, qr_to);
        sql_query = query.replace("{{range}}", &s_hours);
    }
    sql_query = sql_query.replace("{{rnd}}", &gostring::rand_string());
    if ctx.explain {
        sql_query = sql_query.replace("select\n", "explain select\n");
    }
    if ctx.dry_run {
        if ctx.debug >= 0 {
            printf!("{}\n", sql_query);
        } else {
            println!("{}", sql_query);
        }
        return ctx;
    }

    // Connect to Postgres DB
    let con = pg::pg_conn(&ctx);

    // Execute SQL
    let mut rows = pg::query_sql_with_err(&con, &ctx, &sql_query, &[]);

    // Now unknown rows, with unknown types: every value is read as raw bytes
    // (Go scans into `*[]byte`, so times print as RFC3339Nano, floats with
    // `%g`, NULLs as empty strings).
    let columns = rows.column_names();
    let n_columns = columns.len();

    // Get results into `results` array of rows
    let mut results: Vec<Vec<Vec<u8>>> = Vec::new();
    while rows.next() {
        let mut vals: Vec<Vec<u8>> = vec![Vec::new(); n_columns];
        {
            let mut dest: Vec<&mut dyn ScanDest> =
                vals.iter_mut().map(|v| v as &mut dyn ScanDest).collect();
            if let Err(e) = rows.scan(&mut dest) {
                fatal_on_error(e);
            }
        }
        results.push(vals);
    }
    if let Err(e) = rows.err() {
        fatal_on_error(e);
    }
    let row_count = results.len();

    if results.is_empty() {
        printf!("Metric returned no data\n");
        return ctx;
    }

    // Compute column lengths (byte lengths, like Go `len`)
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let mut max_len = column.len();
            for row in &results {
                max_len = max_len.max(row[i].len());
            }
            max_len
        })
        .collect();

    let mut writer = if ctx.csv_file.is_empty() {
        None
    } else {
        // Write output CSV
        match File::create(&ctx.csv_file) {
            Ok(f) => Some(gocsv::Writer::new(f)),
            Err(e) => fatal_on_error(format!("open {}: {}", ctx.csv_file, go_io_error_string(&e))),
        }
    };

    // Upper frame of the header row
    printf_bytes(&frame(&widths, b'/', b'\\'));

    // Header row
    let hdr: Vec<Vec<u8>> = columns.iter().map(|c| c.clone().into_bytes()).collect();
    printf_bytes(&row_line(&hdr, &widths));
    if let Some(w) = writer.as_mut() {
        // Go ignores the writer errors too
        let _ = w.write(&hdr);
    }

    // Frame between header row and data rows
    printf_bytes(&frame(&widths, b'+', b'+'));

    // Data rows loop
    for row in &results {
        if let Some(w) = writer.as_mut() {
            let _ = w.write(row);
        }
        printf_bytes(&row_line(row, &widths));
    }

    // Frame below data rows
    printf_bytes(&frame(&widths, b'\\', b'/'));

    printf!("Rows: {}\n", row_count);
    if let Some(mut w) = writer {
        let _ = w.flush();
        printf!("{} written\n", ctx.csv_file);
    }
    ctx
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    let args: Vec<String> = std::env::args_os()
        .map(|a| a.to_string_lossy().into_owned())
        .collect();
    if args.len() < 2 {
        printf!("Required SQL file name [param1 value1 [param2 value2 ...]]\n");
        printf!("Special replace 'qr' 'period,from,to' is used for {{{{period.alias.name}}}} replacements\n");
        printf!("Example: GHA2DB_QOUT=1 PG_DB=allprj runq metrics/shared/bus_factor.sql qr '1 week,,' {{{{exclude_bots}}}} \"not in ('')\"\n");
        process::exit(1);
    }
    let ctx = runq(&args[1], &args[2..]);
    if ctx.debug >= 0 {
        printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
    }
}
