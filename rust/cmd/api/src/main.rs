//! `api` — Rust port of `cmd/api/api.go`: the DevStats JSON API server.
//!
//! Listens on `GHA2DB_API_HOST` + `GHA2DB_API_PORT` (default `0.0.0.0:8080`),
//! serves `POST /api/v1` with a `{"api": "<Name>", "payload": {…}}` body
//! (17 APIs, see `handlers.rs`), answers with `application/json`, permissive
//! CORS (`rs/cors` `AllowAll`), and logs every request like the Go server.
//! Requires `PG_PASS`, `PG_PASS_RO`, `PG_USER_RO` and `PG_HOST_RO`; reads the
//! enabled projects from `projects.yaml` (`GHA2DB_DATADIR` or `./` with
//! `GHA2DB_LOCAL`).

mod common;
mod handlers;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use devstatscode::consts::*;
use devstatscode::http::{self, html_escape, Handler, Request, Response, ServeMux};
use devstatscode::projects::{is_project_disabled, AllProjects};
use devstatscode::yamlv2::de as yde;
use devstatscode::{fatal_on_err, fatal_on_error, fatalf, gofmt, io, printf, signal, Ctx};
use serde_json::Value;

use common::*;
use handlers::*;

/// Go `apiPayload`: the decoded request body.
#[derive(Default)]
struct ApiPayload {
    api: String,
    payload: Payload,
}

/// Go `requestInfo`.
fn request_info(req: &Request) -> String {
    let agents: Vec<&str> = req
        .headers
        .iter()
        .filter(|(k, _)| k.eq_ignore_ascii_case("User-Agent"))
        .map(|(_, v)| v.as_str())
        .collect();
    let agent = agents.join(", ");
    let path = html_escape(&req.path);
    if !agent.is_empty() {
        format!(
            "IP: {}, agent: {}, method: {}, path: {}",
            req.remote_addr, agent, req.method, path
        )
    } else {
        format!(
            "IP: {}, method: {}, path: {}",
            req.remote_addr, req.method, path
        )
    }
}

/// First byte of the JSON text of `v` (what jsoniter reports as "found").
fn found_char(v: &Value) -> String {
    let c = match v {
        Value::Null => 'n',
        Value::Bool(true) => 't',
        Value::Bool(false) => 'f',
        Value::Number(n) => n.to_string().chars().next().unwrap_or('0'),
        Value::String(_) => '"',
        Value::Array(_) => '[',
        Value::Object(_) => '{',
    };
    c.to_string()
}

/// Case-insensitive field lookup (jsoniter matches struct fields like
/// `encoding/json`: exact key first, then any case-insensitive match).
fn field<'a>(m: &'a serde_json::Map<String, Value>, name: &str) -> Option<&'a Value> {
    m.get(name).or_else(|| {
        m.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })
}

/// `jsoniter.NewDecoder(req.Body).Decode(&pl)`: the first JSON value of the
/// body (anything after it is ignored), `null` and `{}` are the zero payload,
/// `api` must be a string (or null), `payload` an object (or null). The
/// error texts follow jsoniter's leading part (its byte offsets and context
/// dumps are not reproduced).
fn decode_body(req: &Request) -> Result<ApiPayload, String> {
    if req.body.is_empty() {
        return Err(req.body_error.clone().unwrap_or_else(|| "EOF".to_string()));
    }
    let mut stream = serde_json::Deserializer::from_slice(&req.body).into_iter::<Value>();
    let value =
        match stream.next() {
            None => return Err(
                "readObjectStart: expect { or n, but found end of input, error found in #0 byte"
                    .to_string(),
            ),
            Some(Err(e)) => {
                if let Some(be) = &req.body_error {
                    return Err(be.clone());
                }
                return Err(format!("readObjectStart: {}", e));
            }
            Some(Ok(v)) => v,
        };
    match value {
        Value::Null => Ok(ApiPayload::default()),
        Value::Object(m) => {
            let mut pl = ApiPayload::default();
            match field(&m, "api") {
                None | Some(Value::Null) => {}
                Some(Value::String(s)) => pl.api = s.clone(),
                Some(other) => {
                    return Err(format!(
                        "main.apiPayload.API: ReadString: expects \" or n, but found {}",
                        found_char(other)
                    ))
                }
            }
            match field(&m, "payload") {
                None | Some(Value::Null) => {}
                Some(Value::Object(pm)) => pl.payload = Some(pm.clone()),
                Some(other) => {
                    return Err(format!(
                        "main.apiPayload.Payload: ReadMapCB: expect {{ or n, but found {}",
                        found_char(other)
                    ))
                }
            }
            Ok(pl)
        }
        other => Err(format!(
            "readObjectStart: expect {{ or n, but found {}",
            found_char(&other)
        )),
    }
}

/// Go `handleAPI`: the `/api/v1` endpoint.
fn handle_api(req: &Request) -> Response {
    let info = request_info(req);
    let num = num_bg();
    if num == 0 {
        printf!("Request: {}\n", info);
    } else {
        printf!("Request ({} bg runners): {}\n", num, info);
    }
    let mut w = Response::new(200);
    w.set_header("Content-Type", "application/json");
    let mut err = "<nil>".to_string();
    match decode_body(req) {
        Err(e) => {
            return_error("unknown", &mut w, &e);
            err = e;
        }
        Ok(pl) => {
            printf!(
                "Request: {}, Payload: {{API:{} Payload:{}}}\n",
                info,
                pl.api,
                payload_string(&pl.payload)
            );
            match pl.api.as_str() {
                HEALTH => api_health(&mut w, &pl.payload),
                LIST_APIS => api_list_apis(&mut w),
                LIST_PROJECTS => api_list_projects(&mut w),
                REPO_GROUPS => api_repo_groups(&mut w, &pl.payload),
                RANGES => api_ranges(&mut w, &pl.payload),
                COUNTRIES => api_countries(&mut w, &pl.payload),
                COMPANIES => api_companies(&mut w, &pl.payload),
                EVENTS => api_events(&mut w, &pl.payload),
                CUMULATIVE_COUNTS => api_cumulative_counts(&mut w, &pl.payload),
                REPOS => api_repos(&mut w, &pl.payload),
                COMPANIES_TABLE => api_companies_table(&mut w, &pl.payload),
                COM_CONTRIB_REPO_GRP => api_com_contrib_repo_grp(&mut w, &pl.payload),
                COM_STATS_REPO_GRP => api_com_stats_repo_grp(&mut w, &pl.payload),
                DEV_ACT_CNT => api_dev_act_cnt(&mut w, &pl.payload),
                DEV_ACT_CNT_COMP => api_dev_act_cnt_comp(&mut w, &pl.payload),
                SITE_STATS => api_site_stats(&mut w, &pl.payload),
                GITHUB_ID_CONTRIBUTIONS => api_github_id_contributions(&mut w, &pl.payload),
                _ => {
                    err = format!("unknown API '{}'", pl.api);
                    return_error(&format!("unknown:{}", pl.api), &mut w, &err);
                }
            }
        }
    }
    let num = num_bg();
    if num == 0 {
        printf!("Request(exit): {} err:{}\n", info, err);
    } else {
        printf!("Request(exit, {} bg runners): {} err:{}\n", num, info, err);
    }
    w
}

/// `rs/cors` `AllowAll().Handler(h)`.
fn cors_allow_all(inner: Handler) -> Handler {
    const ALLOWED_METHODS: [&str; 6] = ["HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"];
    let method_allowed = |m: &str| m == "OPTIONS" || ALLOWED_METHODS.contains(&m);
    Arc::new(move |req: &Request| {
        let origin = req.header("Origin");
        if req.method == "OPTIONS" && !req.header("Access-Control-Request-Method").is_empty() {
            // Preflight request: standalone, the wrapped handler is not called.
            let mut w = Response::new(204);
            w.set_header(
                "Vary",
                "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
            );
            if origin.is_empty() {
                return w;
            }
            if !method_allowed(req.header("Access-Control-Request-Method")) {
                return w;
            }
            w.set_header("Access-Control-Allow-Origin", "*");
            for (k, v) in &req.headers {
                if k.eq_ignore_ascii_case("Access-Control-Request-Method") {
                    w.headers
                        .push(("Access-Control-Allow-Methods".to_string(), v.clone()));
                }
            }
            let req_headers = req.header("Access-Control-Request-Headers");
            if !req_headers.is_empty() {
                w.set_header("Access-Control-Allow-Headers", req_headers);
            }
            return w;
        }
        // Actual request: the CORS headers are set before the handler runs
        // (the API handlers never touch them).
        let mut w = inner(req);
        w.headers.push(("Vary".to_string(), "Origin".to_string()));
        if !origin.is_empty() && method_allowed(&req.method) {
            w.set_header("Access-Control-Allow-Origin", "*");
        }
        w
    })
}

/// Go `checkEnv`.
fn check_env() {
    for env in ["PG_PASS", "PG_PASS_RO", "PG_USER_RO", "PG_HOST_RO"] {
        if std::env::var(env).unwrap_or_default().is_empty() {
            fatalf!("{} env variable must be set", env);
        }
    }
}

/// Go `readProjects`: the enabled projects of `projects.yaml`.
fn read_projects(ctx: &Ctx) {
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };
    let data = fatal_on_err(io::read_file_raw(format!(
        "{}{}",
        data_prefix, ctx.projects_yaml
    )));
    let projects: AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };
    let mut name_to_db = BTreeMap::new();
    let mut names = Vec::new();
    for (proj_name, proj_data) in &projects.projects {
        if is_project_disabled(ctx, proj_name, proj_data.disabled) {
            continue;
        }
        let db = proj_data.pdb.clone();
        name_to_db.insert(proj_name.clone(), db.clone());
        name_to_db.insert(proj_data.full_name.clone(), db.clone());
        name_to_db.insert(proj_data.pdb.clone(), db);
        names.push(proj_data.full_name.clone());
    }
    if let Ok(mut m) = state().name_to_db.write() {
        *m = name_to_db;
    }
    if let Ok(mut p) = state().projects.write() {
        *p = names;
    }
}

/// Go `%v` of the `os.Signal`s the server exits on.
fn signal_name(sig: i32) -> &'static str {
    match sig {
        signal_hook::consts::SIGINT => "interrupt",
        signal_hook::consts::SIGUSR1 => "user defined signal 1",
        signal_hook::consts::SIGALRM => "alarm clock",
        _ => "signal",
    }
}

/// Go `serveAPI`.
fn serve_api() {
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    printf!("Starting API server\n");
    check_env();
    read_projects(&ctx);
    let mut signals = match signal_hook::iterator::Signals::new([
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGUSR1,
        signal_hook::consts::SIGALRM,
    ]) {
        Ok(s) => s,
        Err(e) => fatal_on_error(devstatscode::error::go_io_error_string(&e)),
    };
    thread::spawn(move || {
        if let Some(sig) = signals.forever().next() {
            printf!("Exiting due to signal {}\n", signal_name(sig));
            std::process::exit(1);
        }
    });
    let mut mux = ServeMux::new();
    mux.handle_func("/api/v1", handle_api);
    let handler = cors_allow_all(mux.into_handler());
    let addr = format!("{}{}", ctx.api_host, ctx.api_port);
    if let Err(e) = http::listen_and_serve(&addr, handler) {
        fatal_on_error(e);
    }
}

fn main() {
    gofmt::mark_process_start();
    serve_api();
    fatalf!("serveAPI exited without error, returning error state anyway");
}
