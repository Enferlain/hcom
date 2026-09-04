<<<<<<< SEARCH
    // --- One-time (kv-tracked) ---

    if inside_tool {
        if !ctx.launcher_participating {
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:start",
                "[tip] Run 'hcom start' to receive notifications/messages from instances",
            );
        }

        if has_close {
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:kill",
                "[tip] Kill agents and close their panes: hcom kill <name1> <name2> ...",
            );
        }

        if !ctx.background {
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:term",
                "[tip] View an agent's screen: hcom term <name> | Inject keystrokes: hcom term inject <name> [text] --enter",
            );
        }

        if is_tmux || ctx.background {
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:sub-blocked",
                "[tip] Get notified when an agent needs approval: hcom events sub --blocked <name>",
            );
        } else {
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:sub-idle",
                "[tip] Get notified when an agent goes idle: hcom events sub --idle <name>",
            );
        }

        once(
            db,
            &mut tips,
            ctx.launcher_name,
            "list:status",
            get_tip("list:status").unwrap_or(""),
        );
    } else {
        once(
            db,
            &mut tips,
            ctx.launcher_name,
            "launch:send",
            "[tip] Send a message to an agent: hcom send @<name> <message>",
        );
        once(
            db,
            &mut tips,
            ctx.launcher_name,
            "launch:list",
            "[tip] Check status: hcom list",
        );
    }
=======
    // --- One-time (kv-tracked) ---

    if inside_tool {
        if has_close {
            // High-level managed workflow
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:managed-wait",
                "[tip] Wait for workflow completion: hcom events --wait --sql stopped:<name>",
            );
        }

        if !ctx.launcher_participating {
            once(
                db,
                &mut tips,
                ctx.launcher_name,
                "launch:start",
                "[tip] Receive their messages: hcom start | Check basic status: hcom list",
            );
        }
    } else {
        once(
            db,
            &mut tips,
            ctx.launcher_name,
            "launch:send",
            "[tip] Send a message to an agent: hcom send @<name> <message>",
        );
        once(
            db,
            &mut tips,
            ctx.launcher_name,
            "launch:list",
            "[tip] Check status: hcom list",
        );
    }
>>>>>>> REPLACE
