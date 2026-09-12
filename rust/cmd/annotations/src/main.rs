//! `annotations` — Rust port of `cmd/annotations/annotations.go`.
//!
//! Reads `projects.yaml` (`GHA2DB_PROJECTS_YAML`, under `./` with
//! `GHA2DB_LOCAL` or `GHA2DB_DATADIR`), finds the `GHA2DB_PROJECT` entry and
//! writes the project's annotations (git tags of its main repository matching
//! `annotation_regexp`, or the fake start/join annotations of a project
//! without a main repository), the CNCF milestone annotations and the quick
//! ranges into the TSDB — and the annotations into the `shared_db` database.
//! Environment, output and exit codes are those of the Go program.

use std::time::Instant;

use devstatscode::annotations::{
    get_annotations, get_fake_annotations, process_annotations, Annotation, Annotations,
};
use devstatscode::projects::AllProjects;
use devstatscode::time as gotime;
use devstatscode::yamlv2::de as yde;
use devstatscode::{fatal_on_err, fatal_on_error, fatalf, io, printf, signal, Ctx};

/// Go `makeAnnotations`: insert TSDB annotations.
fn make_annotations() {
    // Environment context parse
    let mut ctx = Ctx::default();
    ctx.init();
    signal::setup_timeout_signal(&ctx);

    // Needs GHA2DB_PROJECT variable set
    if ctx.project.is_empty() {
        fatalf!("you have to set project via GHA2DB_PROJECT environment variable");
    }

    // Local or cron mode?
    let data_prefix = if ctx.local {
        "./".to_string()
    } else {
        ctx.data_dir.clone()
    };

    // Read defined projects
    let data = fatal_on_err(io::read_file(
        &ctx,
        &format!("{data_prefix}{}", ctx.projects_yaml),
    ));
    let projects: AllProjects = match yde::unmarshal(&data) {
        Ok(p) => p,
        Err(e) => fatal_on_error(e),
    };

    // Get current project's main repo and annotation regexp
    let Some(proj) = projects.projects.get(&ctx.project).cloned() else {
        fatalf!(
            "project '{}' not found in '{}'",
            ctx.project,
            ctx.projects_yaml
        );
    };
    ctx.shared_db = proj.shared_db.clone();
    ctx.project_main_repo = proj.main_repo.clone();

    // Get annotations using git tags and add annotations and quick ranges to TSDB
    if !proj.main_repo.is_empty() {
        let mut annotations = get_annotations(&mut ctx, &proj.main_repo, &proj.annotation_regexp);
        process_annotations(
            &mut ctx,
            &mut annotations,
            &[
                proj.start_date,
                proj.join_date,
                proj.incubating_date,
                proj.graduated_date,
                proj.archived_date,
            ],
        );
    } else if let Some(start_date) = proj.start_date {
        let mut annotations = match proj.join_date {
            Some(join_date) => get_fake_annotations(start_date, join_date),
            None => Annotations {
                annotations: vec![Annotation {
                    name: "Project start".to_string(),
                    description: format!("{} - project starts", gotime::to_ymd_date(start_date)),
                    date: start_date,
                }],
            },
        };
        process_annotations(
            &mut ctx,
            &mut annotations,
            &[
                None,
                None,
                proj.incubating_date,
                proj.graduated_date,
                proj.archived_date,
            ],
        );
    }
}

fn main() {
    devstatscode::error::exit_on_panic();
    let dt_start = Instant::now();
    make_annotations();
    printf!("Time: {}\n", gotime::format_go_duration(dt_start.elapsed()));
}
