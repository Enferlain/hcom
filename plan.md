1. Add `diagnostic_mode: bool` to `LaunchTipsContext` in `src/core/tips.rs`.
2. Extract the call to `tips::print_launch_tips` out of `print_launch_feedback` in `src/commands/launch.rs`. This allows us to call `tips::print_launch_tips` *after* we evaluate launch readiness using `print_inline_launch_readiness`, so we can determine if the launch was successful or if it resulted in a Blocked/Failed state.
3. In `launch.rs` and `resume.rs`, after calling `print_launch_feedback`, call `print_inline_launch_readiness` to get the readiness state.
4. Compute `diagnostic_mode`: it should be true if the readiness state is `Failed` or `Blocked`.
5. Call `tips::print_launch_tips` with the populated context, passing in `diagnostic_mode`.
6. Update `print_launch_tips` in `src/core/tips.rs`:
   - Keep success tips concise.
   - For managed workflows (`inside_tool` and `has_close`), lead with the high-level workflow suggestion (e.g. advertising `hcom run <workflow>`).
   - Restrict the low-level diagnostic commands (`list`, `term`, `kill`, `sub-blocked`, `sub-idle`) to only appear if `diagnostic_mode` is true.
7. Run `cargo test` to ensure both success and failure tests pass. Update tests as necessary since `print_launch_feedback` output might slightly change, or tests mocking this path will need updates.
8. Perform pre-commit checks (`cargo fmt`, `cargo clippy`, etc.).
9. Commit and submit the PR.
