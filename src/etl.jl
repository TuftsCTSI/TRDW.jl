# Workaround for https://github.com/vchuravy/HashArrayMappedTries.jl/issues/11
HashArrayMappedTries.HashState(key::Symbol) =
    HashArrayMappedTries.HashState(key, hash(key, zero(UInt)), 0, 0)

abstract type AbstractTransform end

struct SetTransform <: AbstractTransform
    defs::Vector{Pair{Symbol, FunSQL.SQLNode}}
end

funsql_set(defs::Pair...) =
    SetTransform(Pair{Symbol, FunSQL.SQLNode}[defs...])

struct SnapshotTransform <: AbstractTransform
    names::Vector{Symbol}
end

funsql_snapshot(names...) =
    SnapshotTransform(Symbol[names...])

struct UnsetTransform <: AbstractTransform
    names::Vector{Symbol}
end

funsql_unset(names...) =
    UnsetTransform(Symbol[names...])

struct UnsetAllExceptTransform <: AbstractTransform
    names::Vector{Symbol}
end

funsql_unset_all_except(names...) =
    UnsetAllExceptTransform(Symbol[names...])

struct DoIfSetTransform <: AbstractTransform
    name::Symbol
    t::AbstractTransform
    else_t::AbstractTransform
end

funsql_do_if_set(name, t, else_t = funsql_noop()) =
    DoIfSetTransform(name, t, else_t)

struct DoIfUnsetTransform <: AbstractTransform
    name::Symbol
    t::AbstractTransform
    else_t::AbstractTransform
end

funsql_do_if_unset(name, t, else_t = funsql_noop()) =
    DoIfUnsetTransform(name, t, else_t)

struct IdentityTransform <: AbstractTransform
end

funsql_noop() =
    IdentityTransform()

struct ChainTransform <: AbstractTransform
    t1::AbstractTransform
    t2::AbstractTransform
end

FunSQL.Chain(t1::AbstractTransform, t2::AbstractTransform) =
    ChainTransform(t1, t2)

struct SQLSnapshot
    name::Symbol
end

const TransformEntry = Tuple{Union{FunSQL.SQLTable, FunSQL.SQLNode, SQLSnapshot}, Int}
const TransformSchema = HAMT{Symbol, Int}

struct TransformContext
    name::Symbol
    entries::Vector{TransformEntry}
    schemas::Vector{TransformSchema}
end

function transform!(t::SetTransform, ctx)
    version = lastindex(ctx.schemas)
    schema′ = ctx.schemas[end]
    for (name, node) in t.defs
        push!(ctx.entries, (node, version))
        schema′ = insert(schema′, name, lastindex(ctx.entries))
    end
    push!(ctx.schemas, schema′)
    nothing
end

function transform!(t::SnapshotTransform, ctx)
    version = lastindex(ctx.schemas)
    schema′ = ctx.schemas[end]
    for name in t.names
        index = get(schema′, name, 0)
        if index != 0 && (local def = first(ctx.entries[index])) isa SQLSnapshot && def.name === name
            continue
        end
        push!(ctx.entries, (SQLSnapshot(name), version))
        schema′ = insert(schema′, name, lastindex(ctx.entries))
    end
    push!(ctx.schemas, schema′)
    nothing
end

function transform!(t::UnsetTransform, ctx)
    schema′ = ctx.schemas[end]
    for name in t.names
        schema′ = delete(schema′, name)
    end
    push!(ctx.schemas, schema′)
    nothing
end

function transform!(t::UnsetAllExceptTransform, ctx)
    schema′ = schema = ctx.schemas[end]
    for (name, _) in schema
        name ∉ t.names || continue
        schema′ = delete(schema′, name)
    end
    push!(ctx.schemas, schema′)
    nothing
end

function transform!(t::DoIfSetTransform, ctx)
    t′ = haskey(ctx.schemas[end], t.name) ? t.t : t.else_t
    transform!(t′, ctx)
end

function transform!(t::DoIfUnsetTransform, ctx)
    t′ = !haskey(ctx.schemas[end], t.name) ? t.t : t.else_t
    transform!(t′, ctx)
end

function transform!(::IdentityTransform, ctx)
end

function transform!(t::ChainTransform, ctx)
    transform!(t.t1, ctx)
    transform!(t.t2, ctx)
end

struct TableDefinition
    name_sql::String
    body_sql::String
    is_view::Bool
    is_aux::Bool
    is_tmp::Bool
    reqs::Vector{String}
    etl_hash::String
    etl_time::Int

    TableDefinition(name_sql, body_sql; is_view = false, is_aux = false, is_tmp = false, reqs = String[], etl_hash, etl_time) =
        new(name_sql, body_sql, is_view, is_aux, is_tmp, reqs, etl_hash, etl_time)
end

struct SchemaDefinition
    name_sql::String
    defs::OrderedDict{String, TableDefinition}
    rev_reqs::Dict{String, Vector{String}}

    function SchemaDefinition(name_sql, defs)
        rev_reqs = Dict{String, Vector{String}}()
        for req in keys(defs)
            rev_reqs[req] = String[]
        end
        for (rev_req, def) in defs
            for req in def.reqs
                push!(rev_reqs[req], rev_req)
            end
        end
        new(name_sql, defs, rev_reqs)
    end
end

function build_definition(catalog, ctx)
    cat_m = get_metadata(catalog)
    @assert cat_m !== nothing
    etl_time = cat_m.src_time
    outs = Set{Tuple{Int, Symbol}}()
    outs_closure = Set{Tuple{Int, Symbol}}()
    deps_map = Dict{Tuple{Int, Symbol}, Set{Tuple{Int, Symbol}}}()
    queue = Tuple{Int, Symbol}[]
    for (name, index) in ctx.schemas[end]
        key = (index, name)
        push!(outs, key)
        push!(outs_closure, key)
        push!(queue, key)
    end
    sort!(queue)
    while !isempty(queue)
        (index, name) = key = popfirst!(queue)
        !haskey(deps_map, key) || continue
        (obj, version) = ctx.entries[index]
        deps = Set{Tuple{Int, Symbol}}()
        if obj isa FunSQL.SQLNode
            schema = ctx.schemas[version]
            for dep_name in dependencies(obj, schema)
                haskey(schema, dep_name) || error(dep_name)
                dep_index = schema[dep_name]
                dep_key = (dep_index, dep_name)
                push!(deps, dep_key)
                push!(queue, dep_key)
            end
            if key in outs_closure
                union!(outs_closure, deps)
            end
        elseif obj isa SQLSnapshot
            schema = ctx.schemas[version]
            haskey(schema, obj.name) || error(obj.name)
            dep_index = schema[obj.name]
            dep_key = (dep_index, obj.name)
            push!(deps, dep_key)
            push!(queue, dep_key)
        end
        deps_map[key] = deps
    end
    tables = Dict{Symbol, FunSQL.SQLTable}()
    schema_name_sql = FunSQL.render(catalog, FunSQL.ID(ctx.name))
    defs = OrderedDict{String, TableDefinition}()
    qualifiers = [ctx.name]
    ctes = FunSQL.SQLNode[]
    subs_map = Dict{Int, Set{Int}}()
    sub_to_req = Dict{Int, String}()
    for key in sort!(collect(keys(deps_map)))
        (index, name) = key
        (obj, version) = ctx.entries[index]
        subs = Set{Int}()
        if obj isa FunSQL.SQLTable
            if key in outs
                name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, name))
                body_sql = FunSQL.render(catalog, FunSQL.From(obj))
                etl_hash_ctx = SHA256_CTX()
                update!(etl_hash_ctx, codeunits("CREATE VIEW"))
                update!(etl_hash_ctx, codeunits(body_sql))
                etl_hash = bytes2hex(digest!(etl_hash_ctx))
                defs[name_sql] = TableDefinition(name_sql, body_sql, is_view = true, etl_hash = etl_hash, etl_time = etl_time)
                table = FunSQL.SQLTable(qualifiers = qualifiers, name, columns = body_sql.columns)
                tables[name] = table
            end
            push!(ctes, name => FunSQL.From(obj))
            push!(subs, lastindex(ctes))
        elseif obj isa FunSQL.SQLNode
            for (dep_index, _) in deps_map[key]
                union!(subs, subs_map[dep_index])
            end
            if key in outs
                q = obj
                reqs = Set{String}()
                for k in sort(collect(subs), rev = true)
                    q = FunSQL.With(ctes[k], over = q)
                    req = get(sub_to_req, k, nothing)
                    if req !== nothing
                        push!(reqs, req)
                    end
                end
                reqs = sort!(collect(reqs))
                name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, name))
                body_sql = FunSQL.render(catalog, q)
                @assert body_sql.columns !== nothing name
                etl_hash_ctx = SHA256_CTX()
                for req in reqs
                    update!(etl_hash_ctx, codeunits(defs[req].etl_hash))
                end
                update!(etl_hash_ctx, codeunits("CREATE VIEW"))
                update!(etl_hash_ctx, codeunits(body_sql))
                etl_hash = bytes2hex(digest!(etl_hash_ctx))
                defs[name_sql] = TableDefinition(name_sql, body_sql, is_view = true, reqs = reqs, etl_hash = etl_hash, etl_time = etl_time)
                table = FunSQL.SQLTable(qualifiers = qualifiers, name, columns = body_sql.columns)
                tables[name] = table
            end
            push!(ctes, name => obj)
            push!(subs, lastindex(ctes))
        elseif obj isa SQLSnapshot
            for (dep_index, _) in deps_map[key]
                union!(subs, subs_map[dep_index])
            end
            q = FunSQL.From(obj.name)
            reqs = Set{String}()
            for k in sort(collect(subs), rev = true)
                q = FunSQL.With(ctes[k], over = q)
                req = get(sub_to_req, k, nothing)
                if req !== nothing
                    push!(reqs, req)
                end
            end
            reqs = sort!(collect(reqs))
            is_aux = is_tmp = false
            if key in outs
                snapshot_name = name
            else
                is_aux = true
                prefix = :_aux_
                if key ∉ outs_closure
                    is_tmp = true
                    prefix = :_tmp_
                end
                k = 1
                snapshot_name = Symbol(prefix, name, "_", k)
                while haskey(tables, snapshot_name) || haskey(ctx.schemas[end], snapshot_name)
                    k += 1
                    snapshot_name = Symbol(prefix, name, "_", k)
                end
            end
            name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, snapshot_name))
            body_sql = FunSQL.render(catalog, q)
            @assert body_sql.columns !== nothing name
            etl_hash_ctx = SHA256_CTX()
            for req in reqs
                update!(etl_hash_ctx, codeunits(defs[req].etl_hash))
            end
            update!(etl_hash_ctx, codeunits("CREATE TABLE"))
            update!(etl_hash_ctx, codeunits(body_sql))
            etl_hash = bytes2hex(digest!(etl_hash_ctx))
            defs[name_sql] = TableDefinition(name_sql, body_sql; reqs, is_aux, is_tmp, etl_hash, etl_time)
            table = FunSQL.SQLTable(qualifiers = qualifiers, snapshot_name, columns = body_sql.columns)
            tables[snapshot_name] = table
            empty!(subs)
            push!(ctes, name => FunSQL.From(table))
            push!(subs, lastindex(ctes))
            sub_to_req[lastindex(ctes)] = name_sql
        end
        subs_map[index] = subs
    end
    SchemaDefinition(schema_name_sql, defs)
end

function dependencies(n::FunSQL.SQLNode, schema)
    free = Set{Symbol}()
    bound = Set{Symbol}()
    dependencies!(n, schema, free, bound)
    sort!(collect(free))
end

function dependencies!(n::FunSQL.SQLNode, schema, free, bound)
    dependencies!(n[], schema, free, bound)
end

function dependencies!(ns::Vector{FunSQL.SQLNode}, schema, free, bound)
    for n in ns
        dependencies!(n, schema, free, bound)
    end
end

dependencies!(::Nothing, schema, free, bound) =
    nothing

@generated function dependencies!(n::FunSQL.AbstractSQLNode, schema, free, bound)
    exs = Expr[]
    for f in fieldnames(n)
        t = fieldtype(n, f)
        if t === FunSQL.SQLNode || t === Union{FunSQL.SQLNode, Nothing} || t === Vector{FunSQL.SQLNode}
            ex = quote
                dependencies!(n.$(f), schema, free, bound)
            end
            push!(exs, ex)
        end
    end
    push!(exs, :(return nothing))
    Expr(:block, exs...)
end

function dependencies!(n::FunSQL.FromNode, schema, free, bound)
    source = n.source
    if source isa Symbol && source ∉ bound
        push!(free, source)
    elseif source isa FunSQL.FunctionSource
        dependencies!(source.node, schema, free, bound)
    end
end

function dependencies!(n::Union{FunSQL.WithNode, FunSQL.WithExternalNode}, schema, free, bound)
    dependencies!(n.args, schema, free, bound)
    names = Symbol[name for name in keys(n.label_map) if name ∉ bound]
    for name in names
        push!(bound, name)
    end
    dependencies!(n.over, schema, free, bound)
    for name in names
        pop!(bound, name)
    end
end

function dependencies!(n::FunSQL.OverNode, schema, free, bound)
    dependencies!(FunSQL.With(over = n.arg, args = n.over !== nothing ? FunSQL.SQLNode[n.over] : FunSQL.SQLNode[]), schema, free, bound)
end

function dependencies!(n::IfSetNode, schema, free, bound)
    over = n.over
    node = haskey(schema, n.name) ? n.node : n.else_node
    node = node !== nothing && over !== nothing ? over |> node : node !== nothing ? node : over
    dependencies!(node, schema, free, bound)
end

struct CreateSchemaSpecification
    name::Symbol
    etl::AbstractTransform
    drop_tmp::Bool
end

funsql_create_schema((name, etl)::Pair{<:Union{Symbol, AbstractString}, <:AbstractTransform}; drop_tmp::Bool = true) =
    CreateSchemaSpecification(Symbol(name), etl, drop_tmp)

struct ConnectionPool
    size::Int
    default_catalog::String
    conns::Vector{ODBC.Connection}
    lock::Threads.Condition
    n_qs::Ref{Int}
    n_conns::Ref{Int}

    function ConnectionPool(db; size = 10)
        m = get_metadata(db.catalog)
        @assert m !== nothing
        conns = [db.raw]
        lock = Threads.Condition()
        new(size, m.default_catalog, conns, lock, Ref(0), Ref(1))
    end
end

_is_ready(pool::ConnectionPool) =
    !isempty(pool.conns) || pool.n_conns[] < pool.size

function Base.pop!(pool::ConnectionPool)
    conn = nothing
    @lock pool.lock begin
        pool.n_qs[] += 1
        while !_is_ready(pool)
            wait(pool.lock)
        end
        if !isempty(pool.conns)
            conn = pop!(pool.conns)
        else
            pool.n_conns[] += 1
        end
    end
    if conn === nothing
        try
            conn = connect_to_databricks(catalog = pool.default_catalog)
        catch
            @lock pool.lock pool.n_conns[] -= 1
            rethrow()
        end
    end
    conn
end

function Base.push!(pool::ConnectionPool, conn::ODBC.Connection)
    @lock pool.lock begin
        push!(pool.conns, conn)
        notify(pool.lock, all = false)
    end
end

function execute_ddl(pool, sql, req_tasks = Task[])
    for task in req_tasks
        wait(task)
    end
    conn = pop!(pool)
    try
        @info sql
        sec = @elapsed begin
            DBInterface.execute(conn, sql)
            ODBC.clear!(conn)
        end
        @info "$(split(sql, '\n', limit = 2)[1]): $(round(sec, digits = 1)) seconds"
    finally
        push!(pool, conn)
    end
    nothing
end

function run(db, spec::CreateSchemaSpecification)
    entries = TransformEntry[]
    schema = TransformSchema()
    for (name, t) in db.catalog
        push!(entries, (t, 0))
        schema[name] = lastindex(entries)
    end
    ctx = TransformContext(spec.name, entries, [schema])
    transform!(spec.etl, ctx)
    schema_def = build_definition(db.catalog, ctx)
    pool = ConnectionPool(db)
    existing_tables = Dict{String, TableMetadata}(
        [FunSQL.render(db, FunSQL.ID(t.qualifiers[end:end], t.name)) => (let m = get_metadata(t); @assert m !== nothing; m; end)
         for t in _introspect_schema(db.raw, pool.default_catalog, string(spec.name))])
    matches = Set{String}()
    for def in reverse!(collect(values(schema_def.defs)))
        m = get(existing_tables, def.name_sql, nothing)
        if m !== nothing && m.etl_hash == def.etl_hash && m.etl_time == def.etl_time
            push!(matches, def.name_sql)
        elseif spec.drop_tmp && def.is_tmp && all(rev_req ∈ matches for rev_req in schema_def.rev_reqs[def.name_sql])
            push!(matches, def.name_sql)
        end
    end
    sec = @elapsed @sync begin
        task0 = nothing
        if isempty(matches)
            sql = "DROP SCHEMA IF EXISTS $(schema_def.name_sql) CASCADE"
            task0 = Threads.@spawn execute_ddl($pool, $sql)
            sql = "CREATE SCHEMA $(schema_def.name_sql)"
            task0 = Threads.@spawn execute_ddl($pool, $sql, [$task0])
        else
            for (name_sql, m) in existing_tables
                name_sql ∉ keys(schema_def.defs) || continue
                cur_obj_sql = m.is_view ? "VIEW" : "TABLE"
                sql = "DROP $cur_obj_sql $name_sql"
                Threads.@spawn execute_ddl($pool, $sql)
            end
        end
        task_map = Dict{String, Task}()
        done_tasks_map = Dict{String, Vector{Task}}()
        for def in values(schema_def.defs)
            done_tasks_map[def.name_sql] = Task[]
            def.name_sql ∉ matches || continue
            m = get(existing_tables, def.name_sql, nothing)
            drop_task = nothing
            cmd = "CREATE"
            if isempty(matches) || m === nothing
                drop_task = task0
            elseif m.is_view == def.is_view
                cmd = "CREATE OR REPLACE"
            else
                cur_obj_sql = m.is_view ? "VIEW" : "TABLE"
                sql = "DROP $cur_obj_sql $(def.name_sql)"
                drop_task = Threads.@spawn execute_ddl($pool, $sql)
            end
            obj_sql = def.is_view ? "VIEW" : "TABLE"
            sql = "$cmd $obj_sql $(def.name_sql) AS\n$(def.body_sql)"
            req_tasks = drop_task !== nothing ? [drop_task] : Task[]
            for req in def.reqs
                haskey(task_map, req) || continue
                push!(req_tasks, task_map[req])
            end
            task = Threads.@spawn execute_ddl($pool, $sql, $req_tasks)
            task_map[def.name_sql] = task
            for req in def.reqs
                push!(done_tasks_map[req], task)
            end
            sql = "ALTER $obj_sql $(def.name_sql)\nSET TAGS ('etl_hash' = '$(def.etl_hash)', 'etl_time' = '$(def.etl_time)')"
            task = Threads.@spawn execute_ddl($pool, $sql, [$task])
            push!(done_tasks_map[def.name_sql], task)
        end
        if spec.drop_tmp
            for def in values(schema_def.defs)
                def.is_tmp || continue
                if def.name_sql ∉ matches
                    is_view = def.is_view
                elseif def.name_sql ∈ keys(existing_tables)
                    is_view = existing_tables[def.name_sql].is_view
                else
                    continue
                end
                obj_sql = is_view ? "VIEW" : "TABLE"
                sql = "DROP $obj_sql $(def.name_sql)"
                req_tasks = done_tasks_map[def.name_sql]
                Threads.@spawn execute_ddl($pool, $sql, $req_tasks)
            end
        end
    end
    n_qs = pool.n_qs[]
    if n_qs > 0
        n_conns = pool.n_conns[]
        @info "$n_qs quer$(n_qs == 1 ? "y" : "ies") executed in $(round(sec, digits = 1)) seconds using $n_conns connection$(n_conns == 1 ? "" : "s")"
    end
    tables′ = _introspect_schema(db.raw, nothing, string(spec.name))
    metadata′ = Dict{Symbol, Any}()
    cat′ = FunSQL.SQLCatalog(tables = tables′, dialect = db.catalog.dialect, metadata = metadata′)
    m = get_metadata(db.catalog)
    if m !== nothing
        concept_cache = create_concept_cache(db.raw, get(cat′, :concept, nothing))
        metadata′[:trdw] = CatalogMetadata(m.default_catalog, concept_cache)
    end
    db′ = FunSQL.SQLConnection(db.raw, catalog = cat′)
    db′
end
