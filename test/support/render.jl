using FunSQL

"""
    render_ok(q; catalog = MOCK_CATALOG)

Render `q` against `catalog` and return the `FunSQL.SQLString`. If
rendering fails, or returns something else, print why and return
`nothing`, so a test can check `render_ok(q) !== nothing` and report one
failure per case.

The error is printed inside its own `try`: printing some FunSQL errors
throws (`quoteof(::CustomResolveNode, …)` calls `nameof` on a
`SQLQueryCtor`; see Findings in TESTING_WORKPLAN.md).
"""
function render_ok(q; catalog = MOCK_CATALOG)
    sql = try
        FunSQL.render(catalog, q)
    catch err
        print_render_error(err)
        return nothing
    end
    if !(sql isa FunSQL.SQLString)
        @error "render returned $(typeof(sql)), not a FunSQL.SQLString"
        return nothing
    end
    sql
end

function print_render_error(err)
    msg = try
        sprint(showerror, err)
    catch print_err
        "$(typeof(err)) (printing it threw $(typeof(print_err)))"
    end
    @error "render failed: $msg"
end
