//! Single key press reader — port of `mgetc.go`.

use std::io::Read;

use crate::context::Ctx;
use crate::error::fatalf;

/// Wait for a single key press and return it; `ctx.mgetc` (from `GHA2DB_MGETC`)
/// overrides the interactive read so batch runs never block.
pub fn mgetc(ctx: &Ctx) -> String {
    if !ctx.mgetc.is_empty() {
        return ctx.mgetc.clone();
    }
    let mut b = [0u8; 1];
    match std::io::stdin().read(&mut b) {
        Ok(0) => fatalf(format_args!("EOF")),
        Ok(_) => String::from_utf8_lossy(&b).into_owned(),
        Err(e) => fatalf(format_args!("{}", crate::error::go_io_error_string(&e))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::test_support::env_lock;

    #[test]
    fn returns_context_override() {
        let _g = env_lock();
        let mut ctx = Ctx::default();
        ctx.init();
        ctx.test_mode = true;
        ctx.mgetc = "y".to_string();
        assert_eq!(mgetc(&ctx), "y");
    }
}
