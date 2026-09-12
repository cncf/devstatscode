//! `webhook` — Travis CI webhook receiver deploying DevStats on successful
//! builds (port of `cmd/webhook/webhook.go`).
//!
//! Listens on `GHA2DB_WHHOST:GHA2DB_WHPORT` (`127.0.0.1:1982`) and serves
//! `GHA2DB_WHROOT` (`/hook`). Every request is a Travis notification whose
//! `payload=<json>` body (signature-verified against the Travis public key
//! unless `GHA2DB_SKIP_VERIFY_PAYLOAD` is set) must describe a passed `push`
//! build of `cncf/devstats` or `cncf/devstatscode` on an allowed branch;
//! then, serialised through `/tmp/webhook.pid`, the tool runs `git checkout
//! <branch>`, `git pull`, `make` (devstats only) and `make install` in
//! `GHA2DB_PROJECT_ROOT`, plus `./devel/deploy_all.sh` for `[deploy]`
//! commit messages.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine;
use devstatscode::consts::{DEVSTATS, DEVSTATS_CODE};
use devstatscode::error::{fatal_on_error, fatal_on_error_in_handler, go_io_error_string};
use devstatscode::http::{self, Request, Response, ServeMux};
use devstatscode::time as gotime;
use devstatscode::{exec, gofmt, httpclient, printf, signal, Ctx};
use rsa::pkcs8::DecodePublicKey;
use serde_json::Value;
use sha1::Digest;

/// Travis CI configuration endpoint publishing the webhook public key.
const TRAVIS_CONFIG_URL: &str = "https://api.travis-ci.org/config";

/// The PID file serialising deployments.
const PID_FILE: &str = "/tmp/webhook.pid";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Repository {
    name: String,
    owner_name: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Payload {
    branch: String,
    result: i64,
    result_message: String,
    typ: String,
    author_email: String,
    author_name: String,
    message: String,
    repo: Repository,
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Go's `encoding/json`/jsoniter field lookup: the exact key first, then a
/// case-insensitive match.
fn field<'a>(obj: &'a serde_json::Map<String, Value>, name: &str) -> Option<&'a Value> {
    obj.get(name).or_else(|| {
        obj.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    })
}

fn string_field(
    obj: &serde_json::Map<String, Value>,
    name: &str,
    path: &str,
) -> Result<String, String> {
    match field(obj, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(s)) => Ok(s.clone()),
        Some(other) => Err(format!(
            "{}.{}: expected string, but found {}",
            path,
            name,
            json_type_name(other)
        )),
    }
}

/// Decode the Travis payload like `jsoniter.Unmarshal` into the Go struct:
/// unknown keys are ignored, `null` leaves the zero value, key matching is
/// case-insensitive, and a value of the wrong type is an error.
fn decode_payload(json: &str) -> Result<Payload, String> {
    let value: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let Value::Object(obj) = value else {
        return Err(format!(
            "main.payload: ReadObject: expect {{ or n, but found {}",
            json_type_name(&value)
        ));
    };
    let mut pl = Payload {
        branch: string_field(&obj, "branch", "main.payload")?,
        result_message: string_field(&obj, "result_message", "main.payload")?,
        typ: string_field(&obj, "type", "main.payload")?,
        author_email: string_field(&obj, "author_email", "main.payload")?,
        author_name: string_field(&obj, "author_name", "main.payload")?,
        message: string_field(&obj, "message", "main.payload")?,
        ..Default::default()
    };
    match field(&obj, "result") {
        None | Some(Value::Null) => {}
        Some(Value::Number(n)) => match n.as_i64() {
            Some(i) => pl.result = i,
            None => {
                return Err(format!(
                    "main.payload.result: readInt64: cannot decode {}",
                    n
                ))
            }
        },
        Some(other) => {
            return Err(format!(
                "main.payload.result: expected number, but found {}",
                json_type_name(other)
            ))
        }
    }
    match field(&obj, "repository") {
        None | Some(Value::Null) => {}
        Some(Value::Object(repo)) => {
            pl.repo.name = string_field(repo, "name", "main.payload.repository")?;
            pl.repo.owner_name = string_field(repo, "owner_name", "main.payload.repository")?;
        }
        Some(other) => {
            return Err(format!(
                "main.payload.repository: ReadObject: expect {{ or n, but found {}",
                json_type_name(other)
            ))
        }
    }
    Ok(pl)
}

/// `{"message": "<m>"}` with the message JSON-quoted (Go `json.Marshal`).
fn json_message(m: &str) -> String {
    let quoted = devstatscode::json::to_pretty_json(&m)
        .map(|b| String::from_utf8_lossy(&b).into_owned())
        .unwrap_or_else(|_| "\"\"".to_string());
    format!("{{\"message\": {}}}", quoted)
}

fn respond_with_error(m: &str) -> Response {
    Response::with_body(401, "application/json", json_message(m))
}

fn respond_with_success(m: &str) -> Response {
    Response::with_body(200, "application/json", json_message(m))
}

fn payload_signature(r: &Request) -> Result<Vec<u8>, String> {
    let signature = r.header("Signature");
    base64::engine::general_purpose::STANDARD
        .decode(signature)
        .map_err(|_| "cannot decode signature".to_string())
}

fn payload_digest(payload: &str) -> Vec<u8> {
    sha1::Sha1::digest(payload.as_bytes()).to_vec()
}

/// Go `pem.Decode` + `x509.ParsePKIXPublicKey` + the RSA type assertion:
/// the first `PUBLIC KEY` PEM block (anything before it is skipped) must hold
/// an RSA SubjectPublicKeyInfo.
fn parse_public_key(key: &str) -> Result<rsa::RsaPublicKey, String> {
    let invalid = || "invalid public key".to_string();
    let begin = key.find("-----BEGIN ").ok_or_else(invalid)?;
    let rest = &key[begin + "-----BEGIN ".len()..];
    let type_end = rest.find("-----").ok_or_else(invalid)?;
    let block_type = &rest[..type_end];
    if block_type != "PUBLIC KEY" {
        return Err(invalid());
    }
    let body = &rest[type_end + 5..];
    let end_marker = format!("-----END {}-----", block_type);
    let end = body.find(&end_marker).ok_or_else(invalid)?;
    let b64: String = body[..end].chars().filter(|c| !c.is_whitespace()).collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|_| invalid())?;
    rsa::RsaPublicKey::from_public_key_der(&der).map_err(|_| invalid())
}

fn travis_public_key() -> Result<rsa::RsaPublicKey, String> {
    let resp = httpclient::get(TRAVIS_CONFIG_URL)
        .map_err(|_| "cannot fetch travis public key".to_string())?;
    // Go decodes the first JSON value of the body (trailing data is ignored).
    let mut stream = serde_json::Deserializer::from_slice(&resp.body).into_iter::<Value>();
    let config: Value = match stream.next() {
        Some(Ok(v)) => v,
        _ => return Err("cannot decode travis public key".to_string()),
    };
    let public_key = config
        .get("config")
        .and_then(|c| c.get("notifications"))
        .and_then(|n| n.get("webhook"))
        .and_then(|w| w.get("public_key"));
    let public_key = match public_key {
        None | Some(Value::Null) => "",
        Some(Value::String(s)) => s.as_str(),
        Some(_) => return Err("cannot decode travis public key".to_string()),
    };
    parse_public_key(public_key)
}

/// Go `rsa.VerifyPKCS1v15(key, crypto.SHA1, hashed, sig)`.
fn verify_pkcs1v15_sha1(key: &rsa::RsaPublicKey, hashed: &[u8], sig: &[u8]) -> Result<(), String> {
    // DigestInfo prefix for SHA-1 (Go's `hashPrefixes[crypto.SHA1]`).
    const SHA1_PREFIX: [u8; 15] = [
        0x30, 0x21, 0x30, 0x09, 0x06, 0x05, 0x2b, 0x0e, 0x03, 0x02, 0x1a, 0x05, 0x00, 0x04, 0x14,
    ];
    let scheme = rsa::Pkcs1v15Sign {
        hash_len: Some(20),
        prefix: Box::new(SHA1_PREFIX),
    };
    key.verify(scheme, hashed, sig)
        .map_err(|_| "crypto/rsa: verification error".to_string())
}

/// `checkError`: report the error (log + stderr when asked) and produce the
/// 401 response; `None` when there is no error.
fn check_error(is_error: bool, log_it: bool, err: Option<String>) -> Option<Response> {
    let err = err?;
    if log_it {
        if is_error {
            printf!("webhook: error: {}\n", err);
            eprintln!("webhook: error: {}", err);
        } else {
            printf!("webhook: warning: {}\n", err);
        }
    }
    Some(respond_with_error(&format!("webhook: {}", err)))
}

/// `checkDeployEnvError`: the variables the `[deploy]` mode needs.
fn check_deploy_env_error() -> Option<Response> {
    let mut err_msg = String::new();
    if std::env::var("PG_PASS").unwrap_or_default().is_empty() {
        err_msg.push_str("Environment variable 'PG_PASS' must be set in [deploy] mode\n");
    }
    if !err_msg.is_empty() {
        printf!("webhook: error: {}\n", err_msg);
        eprintln!("webhook: error: {}", err_msg);
        return Some(respond_with_error(&format!("webhook: {}", err_msg)));
    }
    None
}

/// `successPayload`: is this a payload to deploy?
fn success_payload(ctx: &Ctx, pl: &Payload) -> bool {
    if (pl.repo.name != DEVSTATS && pl.repo.name != DEVSTATS_CODE) || pl.repo.owner_name != "cncf" {
        return false;
    }
    if pl.message.contains("[no deploy]") || pl.message.contains("[wip]") {
        return false;
    }
    ctx.deploy_statuses.contains(&pl.result_message)
        && ctx.deploy_results.contains(&pl.result)
        && ctx.deploy_types.contains(&pl.typ)
        && ctx.deploy_branches.contains(&pl.branch)
}

fn exec(ctx: &Ctx, args: &[&str], env: &BTreeMap<String, String>) -> Option<String> {
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    exec::exec_command(ctx, &args, env)
        .err()
        .map(|e| e.to_string())
}

/// `webhookHandler`: receive a Travis CI webhook and deploy.
fn webhook_handler(r: &Request) -> Response {
    let dt_start = Instant::now();
    let mut ctx = Ctx::default();
    ctx.init();

    printf!(
        "WebHook processing event {} at {}\n",
        r.remote_addr,
        gofmt::time_now()
    );
    printf!(
        "WebHook config is Host:{} Port:{} Root:{}\n",
        ctx.web_hook_host,
        ctx.web_hook_port,
        ctx.web_hook_root
    );

    let json_str: String;
    if ctx.check_payload {
        let key = match travis_public_key() {
            Ok(k) => k,
            Err(e) => return check_error(true, true, Some(e)).unwrap(),
        };
        let signature = match payload_signature(r) {
            Ok(s) => s,
            Err(e) => return check_error(true, true, Some(e)).unwrap(),
        };
        json_str = r.form_value("payload");
        let digest = payload_digest(&json_str);
        if let Err(e) = verify_pkcs1v15_sha1(&key, &digest, &signature) {
            printf!("webhook: unauthorized payload: {}\n", e);
            return respond_with_error("unauthorized payload");
        }
    } else {
        // Go: `ioutil.ReadAll(r.Body)` fails on malformed chunked bodies etc.
        if let Some(e) = &r.body_error {
            return check_error(true, true, Some(e.clone())).unwrap();
        }
        let s_body = match devstatscode::gourl::query_unescape_bytes(&r.body) {
            Ok(b) => String::from_utf8_lossy(&b).into_owned(),
            Err(e) => return check_error(true, true, Some(e)).unwrap(),
        };
        // Body is expected to be "payload=<json>"
        if s_body.len() < 8 {
            return check_error(true, true, Some("payload too short".to_string())).unwrap();
        }
        json_str = if s_body.is_char_boundary(8) {
            s_body[8..].to_string()
        } else {
            String::from_utf8_lossy(&s_body.as_bytes()[8..]).into_owned()
        };
    }
    let payload = match decode_payload(&json_str) {
        Ok(p) => p,
        Err(e) => return check_error(true, true, Some(e)).unwrap(),
    };
    printf!(
        "WebHook: repo: {}/{}, allowed: cncf/devstats, cncf/devstatscode\n",
        payload.repo.owner_name,
        payload.repo.name
    );
    printf!(
        "WebHook: branch: {}, allowed branches: {}\n",
        payload.branch,
        gofmt::slice(&ctx.deploy_branches)
    );
    printf!(
        "WebHook: status: {}, allowed statuses: {}\n",
        payload.result_message,
        gofmt::slice(&ctx.deploy_statuses)
    );
    printf!(
        "WebHook: type: {}, allowed types: {}\n",
        payload.typ,
        gofmt::slice(&ctx.deploy_types)
    );
    printf!(
        "WebHook: result: {}, allowed results: {}\n",
        payload.result,
        gofmt::slice(&ctx.deploy_results)
    );
    printf!(
        "WebHook: author: name: {}, email: {}\n",
        payload.author_name,
        payload.author_email
    );
    printf!("WebHook: message: {}\n", payload.message);
    if !success_payload(&ctx, &payload) {
        return check_error(
            false,
            false,
            Some(
                "webhook: skipping deploy due to wrong status, result, branch, message and/or type"
                    .to_string(),
            ),
        )
        .unwrap();
    }
    if let Err(e) = std::env::set_current_dir(&ctx.project_root) {
        return check_error(
            true,
            true,
            Some(format!(
                "chdir {}: {}",
                ctx.project_root,
                go_io_error_string(&e)
            )),
        )
        .unwrap();
    }

    // Create PID file (if not exists)
    // If PID file exists, wait
    let pid = std::process::id();
    let mut trials = 0;
    let max_trials = 3800;
    loop {
        let mut opts = OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o700);
        }
        match opts.open(PID_FILE) {
            Ok(mut f) => {
                if let Err(e) = write!(f, "{}", pid).and_then(|_| f.flush()) {
                    fatal_on_error_in_handler(format!(
                        "write {}: {}",
                        PID_FILE,
                        go_io_error_string(&e)
                    ));
                }
                break;
            }
            Err(_) => {
                if trials == 0 {
                    printf!(
                        "Another `webhook` instance is running, PID file '{}' exists, waiting\n",
                        PID_FILE
                    );
                }
                thread::sleep(Duration::from_secs(1));
                trials += 1;
                if trials >= max_trials {
                    printf!(
                        "Another `webhook` instance is running, PID file '{}' exists, tried {} times, exiting\n",
                        PID_FILE, trials
                    );
                    return Response::new(200);
                }
            }
        }
    }
    if trials > 0 {
        printf!(
            "Another `webhook` instance was running, waited {} seconds\n",
            trials
        );
    }

    let resp = deploy(&mut ctx, &payload, dt_start);

    // Remove PID file when finished (Go: deferred `lib.FatalOnError`, whose
    // panic `net/http` recovers — the server stays up)
    if let Err(e) = std::fs::remove_file(PID_FILE) {
        fatal_on_error_in_handler(format!("remove {}: {}", PID_FILE, go_io_error_string(&e)));
    }
    resp
}

/// The deployment steps (`make install`, optionally `./devel/deploy_all.sh`).
fn deploy(ctx: &mut Ctx, payload: &Payload, dt_start: Instant) -> Response {
    let no_env = BTreeMap::new();
    printf!("WebHook: deploying via `{}`\n", "make install");
    ctx.exec_fatal = false;
    printf!("WebHook: git checkout {}\n", payload.branch);
    if let Some(resp) = check_error(
        true,
        true,
        exec(ctx, &["git", "checkout", &payload.branch], &no_env),
    ) {
        return resp;
    }
    printf!("WebHook: {}\n", "git pull");
    if let Some(resp) = check_error(true, true, exec(ctx, &["git", "pull"], &no_env)) {
        return resp;
    }
    if payload.repo.name != DEVSTATS_CODE {
        printf!("WebHook: {}\n", "make");
        if let Some(resp) = check_error(true, true, exec(ctx, &["make"], &no_env)) {
            return resp;
        }
    }
    printf!("WebHook: {}\n", "make install");
    if let Some(resp) = check_error(true, true, exec(ctx, &["make", "install"], &no_env)) {
        return resp;
    }
    let mut deployed_by = "'make install'".to_string();
    if ctx.full_deploy && payload.message.contains("[deploy]") {
        if let Some(resp) = check_deploy_env_error() {
            return resp;
        }
        printf!("WebHook: {}\n", "./devel/deploy_all.sh");
        let mut env = BTreeMap::new();
        env.insert("FROM_WEBHOOK".to_string(), "1".to_string());
        if let Some(resp) = check_error(true, true, exec(ctx, &["./devel/deploy_all.sh"], &env)) {
            return resp;
        }
        printf!("WebHook: {} succeeded\n", "./devel/deploy_all.sh");
        deployed_by.push_str(", './devel/deploy_all.sh'");
    }
    printf!(
        "WebHook: deployed via {} in {}\n",
        deployed_by,
        gotime::format_go_duration(dt_start.elapsed())
    );
    respond_with_success("ok")
}

fn main() {
    gofmt::mark_process_start();
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);
    if ctx.project_root.is_empty() {
        printf!(
            "You need to define reposiory path via GHA2DB_PROJECT_ROOT=/path/to/repo {}\n",
            std::env::args().next().unwrap_or_default()
        );
        return;
    }

    // Start webhook server
    // WebHookHost defaults to "127.0.0.1"
    // WebHookPort defaults to ":1982"
    // WebHookRoot defaults to "/hook"
    let mut mux = ServeMux::new();
    mux.handle_func(&ctx.web_hook_root, webhook_handler);
    let addr = format!("{}{}", ctx.web_hook_host, ctx.web_hook_port);
    if let Err(e) = http::listen_and_serve(&addr, mux.into_handler()) {
        fatal_on_error(e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_with(branches: &[&str]) -> Ctx {
        Ctx {
            deploy_branches: branches.iter().map(|s| s.to_string()).collect(),
            deploy_statuses: vec!["Passed".into(), "Fixed".into()],
            deploy_results: vec![0],
            deploy_types: vec!["push".into()],
            ..Default::default()
        }
    }

    fn good() -> Payload {
        Payload {
            branch: "master".into(),
            result: 0,
            result_message: "Passed".into(),
            typ: "push".into(),
            author_email: "a@b.c".into(),
            author_name: "A".into(),
            message: "Fix".into(),
            repo: Repository {
                name: "devstats".into(),
                owner_name: "cncf".into(),
            },
        }
    }

    #[test]
    fn success_payload_rules() {
        let ctx = ctx_with(&["master"]);
        assert!(success_payload(&ctx, &good()));
        let mut p = good();
        p.repo.name = "devstatscode".into();
        assert!(success_payload(&ctx, &p));
        p.repo.name = "other".into();
        assert!(!success_payload(&ctx, &p));
        let mut p = good();
        p.repo.owner_name = "lukaszgryglicki".into();
        assert!(!success_payload(&ctx, &p));
        for msg in ["[no deploy] x", "x [wip]"] {
            let mut p = good();
            p.message = msg.into();
            assert!(!success_payload(&ctx, &p), "{msg}");
        }
        let mut p = good();
        p.result_message = "Failed".into();
        assert!(!success_payload(&ctx, &p));
        let mut p = good();
        p.result = 1;
        assert!(!success_payload(&ctx, &p));
        let mut p = good();
        p.typ = "pull_request".into();
        assert!(!success_payload(&ctx, &p));
        let mut p = good();
        p.branch = "dev".into();
        assert!(!success_payload(&ctx, &p));
        assert!(success_payload(&ctx_with(&["dev", "master"]), &p));
    }

    #[test]
    fn decode_payload_like_jsoniter() {
        let p = decode_payload(
            r#"{"branch":"master","result":0,"result_message":"Passed","type":"push","author_email":"a@b.c","author_name":"A","message":"Fix","repository":{"name":"devstats","owner_name":"cncf"},"extra":[1,2]}"#,
        )
        .unwrap();
        assert_eq!(p, good());
        // null → zero value, unknown keys ignored, case-insensitive keys.
        let p =
            decode_payload(r#"{"Branch":"dev","result":null,"repository":null,"x":1}"#).unwrap();
        assert_eq!(p.branch, "dev");
        assert_eq!(p.result, 0);
        assert_eq!(p.repo, Repository::default());
        assert_eq!(decode_payload("{}").unwrap(), Payload::default());
        // type errors
        assert!(decode_payload(r#"{"branch":1}"#).is_err());
        assert!(decode_payload(r#"{"result":"0"}"#).is_err());
        assert!(decode_payload(r#"{"result":1.5}"#).is_err());
        assert!(decode_payload(r#"{"repository":"x"}"#).is_err());
        assert!(decode_payload("[]").is_err());
        assert!(decode_payload("").is_err());
        assert!(decode_payload("{} x").is_err());
        assert_eq!(decode_payload(r#"{"result":-7}"#).unwrap().result, -7);
    }

    #[test]
    fn json_message_is_valid_json() {
        assert_eq!(json_message("ok"), "{\"message\": \"ok\"}");
        let m = json_message("webhook: exec: \"make\": not found\nline2 <&>");
        let v: Value = serde_json::from_str(&m).unwrap();
        assert_eq!(
            v["message"],
            "webhook: exec: \"make\": not found\nline2 <&>"
        );
        assert!(m.contains("\\u003c\\u0026\\u003e"), "{m}");
    }

    #[test]
    fn signature_and_digest() {
        let mut r = Request::default();
        r.headers.push(("signature".into(), "aGVsbG8=".into()));
        assert_eq!(payload_signature(&r).unwrap(), b"hello");
        r.headers[0].1 = "!!!".into();
        assert_eq!(
            payload_signature(&r).unwrap_err(),
            "cannot decode signature"
        );
        assert_eq!(payload_signature(&Request::default()).unwrap(), b"");
        assert_eq!(
            payload_digest("abc"),
            [
                0xa9, 0x99, 0x3e, 0x36, 0x47, 0x06, 0x81, 0x6a, 0xba, 0x3e, 0x25, 0x71, 0x78, 0x50,
                0xc2, 0x6c, 0x9c, 0xd0, 0xd8, 0x9d
            ]
        );
    }

    #[test]
    fn public_key_parsing_and_verification() {
        assert_eq!(parse_public_key("").unwrap_err(), "invalid public key");
        assert_eq!(
            parse_public_key("-----BEGIN RSA PUBLIC KEY-----\nAAAA\n-----END RSA PUBLIC KEY-----")
                .unwrap_err(),
            "invalid public key"
        );
        assert_eq!(
            parse_public_key("-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----")
                .unwrap_err(),
            "invalid public key"
        );
        // A real key/signature pair generated with openssl (RSA 1024, SHA-1
        // PKCS#1 v1.5 over the payload below).
        let pem = "leading text is skipped like Go's pem.Decode\n\
-----BEGIN PUBLIC KEY-----\n\
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCj5gQh7CZFepr9dgvp/stBLlE0\n\
5mMYQgQWYVf+Qw/KJE5UUdUT5hVTZlLd249dXm1k7NPoh8YMbX/OHLtkIEkZD9WQ\n\
99wBDA2yQ2Kg6Q384DQVnJYvtCy5wgL2XhacvorokGKndVvJs+/EfXmfJqfIyi9I\n\
K/EAtYU6/ySSsb5ejQIDAQAB\n\
-----END PUBLIC KEY-----\n";
        let key = parse_public_key(pem).unwrap();
        let payload = "{\"branch\":\"master\"}";
        let sig = base64::engine::general_purpose::STANDARD
            .decode(SIG_B64)
            .unwrap();
        assert!(verify_pkcs1v15_sha1(&key, &payload_digest(payload), &sig).is_ok());
        assert_eq!(
            verify_pkcs1v15_sha1(&key, &payload_digest("{\"branch\":\"dev\"}"), &sig).unwrap_err(),
            "crypto/rsa: verification error"
        );
        assert!(verify_pkcs1v15_sha1(&key, &payload_digest(payload), &sig[1..]).is_err());
    }

    const SIG_B64: &str = "hHpbkbO/OCJ8o8UTKj6wGVhptP2OaBq8eWRICVjdTVYaSCW8Mrm3hYmE1mKRk8HNeriNxo5IVcEavbJhJRPMoyauOHImZmWToSW7c161RZj84hkNW639modfYmS2XIJVhLsrzNuTTIRQF7vxtFK5rGkWcfkqX+ZAXW0OmOYKa04=";
}
