using FunSQL

"""
    render_ok(q; catalog = MOCK_CATALOG, context = nothing)

Render `q` against `catalog` and return the `FunSQL.SQLString`. If
rendering fails, or returns something else, print why and return
`nothing`, so a test can check `render_ok(q) !== nothing` and report one
failure per case. `context`, if given, starts the message (for example the
name of the case), since a failed `@test` doesn't show the log.

The error is printed inside its own `try`: printing some FunSQL errors
throws (`quoteof(::CustomResolveNode, …)` calls `nameof` on a
`SQLQueryCtor`).
"""
function render_ok(q; catalog = MOCK_CATALOG, context = nothing)
    sql = try
        FunSQL.render(catalog, q)
    catch err
        print_render_error(err; context)
        return nothing
    end
    if !(sql isa FunSQL.SQLString)
        @error with_context(context, "render returned $(typeof(sql)), not a FunSQL.SQLString")
        return nothing
    end
    sql
end

function print_render_error(err; what = "render failed", context = nothing)
    msg = try
        sprint(showerror, err)
    catch print_err
        "$(typeof(err)) (printing it threw $(typeof(print_err)))"
    end
    @error with_context(context, "$what: $msg")
end

with_context(context, msg) = context === nothing ? msg : "$context: $msg"

"""
    build_and_render(build; catalog = MOCK_CATALOG, context = nothing)

Call `build()` and render the query it returns with `render_ok`. If
`build()` throws, or returns something other than a `FunSQL.SQLQuery`,
print why and return `nothing`, as `render_ok` does for a failed render.
Call it inside `with_test_env`, since building some queries reads the
environment or `TRDW.json`.
"""
function build_and_render(build; catalog = MOCK_CATALOG, context = nothing)
    q = try
        build()
    catch err
        print_render_error(err; what = "building the query failed", context)
        return nothing
    end
    if !(q isa FunSQL.SQLQuery)
        @error with_context(context, "built a $(typeof(q)), not a FunSQL.SQLQuery")
        return nothing
    end
    render_ok(q; catalog, context)
end
