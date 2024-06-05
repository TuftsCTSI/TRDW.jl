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
        if name ∈ schema′ && (local def = first(entries[schema′[name]]); def isa SQLSnapshot && def.name === name)
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
    reqs::Set{String}

    TableDefinition(name_sql, body_sql; is_view = false, reqs = Set{String}()) =
        new(name_sql, body_sql, is_view, reqs)
end

struct SchemaDefinition
    name_sql::String
    defs::OrderedDict{String, TableDefinition}
end

function build_definition(catalog, ctx)
    outs = Set{Tuple{Int, Symbol}}()
    deps_map = Dict{Tuple{Int, Symbol}, Set{Tuple{Int, Symbol}}}()
    queue = Tuple{Int, Symbol}[]
    for (name, index) in ctx.schemas[end]
        key = (index, name)
        push!(outs, key)
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
    for key in sort(collect(keys(deps_map)))
        (index, name) = key
        (obj, version) = ctx.entries[index]
        subs = Set{Int}()
        if obj isa FunSQL.SQLTable
            if key in outs
                name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, name))
                body_sql = FunSQL.render(catalog, FunSQL.From(obj))
                defs[name_sql] = TableDefinition(name_sql, body_sql, is_view = true)
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
                name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, name))
                body_sql = FunSQL.render(catalog, q)
                @assert body_sql.columns !== nothing name
                defs[name_sql] = TableDefinition(name_sql, body_sql, is_view = true, reqs = reqs)
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
            if key in outs
                snapshot_name = name
            else
                k = 1
                snapshot_name = Symbol("_snapshot_", name, "_", k)
                while haskey(tables, snapshot_name) || haskey(ctx.schemas[end], snapshot_name)
                    k += 1
                    snapshot_name = Symbol("_snapshot_", name, "_", k)
                end
            end
            name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, snapshot_name))
            body_sql = FunSQL.render(catalog, q)
            @assert body_sql.columns !== nothing name
            defs[name_sql] = TableDefinition(name_sql, body_sql, reqs = reqs)
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

function dependencies!(n::IfSetNode, schema, free, bound)
    over = n.over
    node = haskey(schema, n.name) ? n.node : n.else_node
    node = node !== nothing && over !== nothing ? over |> node : node !== nothing ? node : over
    dependencies!(node, schema, free, bound)
end

struct CreateSchemaSpecification
    name::Symbol
    etl::AbstractTransform
end

funsql_create_schema((name, etl)::Pair{<:Union{Symbol, AbstractString}, <:AbstractTransform}) =
    CreateSchemaSpecification(Symbol(name), etl)

struct ConnectionPool
    default_catalog::String
    conns::Vector{ODBC.Connection}
    lock::ReentrantLock

    function ConnectionPool(db)
        default_catalog = db.catalog.metadata !== nothing ? get(db.catalog.metadata, :default_catalog, nothing) : nothing
        conns = [db.raw]
        lock = ReentrantLock()
        new(default_catalog, conns, lock)
    end
end

function Base.pop!(pool::ConnectionPool)
    conn = nothing
    @lock pool.lock begin
        if !isempty(pool.conns)
            conn = pop!(pool.conns)
        end
    end
    @something conn connect_to_databricks(catalog = pool.default_catalog)
end

function Base.push!(pool::ConnectionPool, conn::ODBC.Connection)
    @lock pool.lock push!(pool.conns, conn)
end

function execute_ddl(pool, sql, req_tasks = Task[])
    for task in req_tasks
        wait(task)
    end
    conn = pop!(pool)
    @info sql
    DBInterface.execute(conn, sql)
    push!(pool, conn)
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
    @sync begin
        sql = "DROP SCHEMA IF EXISTS $(schema_def.name_sql) CASCADE"
        task0 = Threads.@spawn execute_ddl($pool, $sql)
        sql = "CREATE SCHEMA $(schema_def.name_sql)"
        task0 = Threads.@spawn execute_ddl($pool, $sql, [$task0])
        task_map = Dict{String, Task}()
        for def in values(schema_def.defs)
            sql = "CREATE $(def.is_view ? "VIEW" : "TABLE") $(def.name_sql) AS $(def.body_sql)"
            req_tasks = [task0]
            for req in def.reqs
                push!(req_tasks, task_map[req])
            end
            task_map[def.name_sql] = Threads.@spawn execute_ddl($pool, $sql, $req_tasks)
        end
    end
    tables′ = _introspect_schema(db.raw, nothing, string(spec.name))
    metadata′ = Dict{Symbol, Any}()
    cat′ = FunSQL.SQLCatalog(tables = tables′, dialect = db.catalog.dialect, metadata = metadata′)
    metadata = @something db.catalog.metadata Dict{Symbol, Any}()
    metadata′[:default_catalog] = get(metadata, :default_catalog, nothing)
    metadata′[:concept_cache] = create_concept_cache(db.raw, get(cat′, :concept, nothing))
    metadata′[:created] = Dates.now()
    db′ = FunSQL.SQLConnection(db.raw, catalog = cat′)
    db′
end
