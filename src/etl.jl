# Workaround for https://github.com/vchuravy/HashArrayMappedTries.jl/issues/11
HashArrayMappedTries.HashState(key::Symbol) =
    HashArrayMappedTries.HashState(key, hash(key, zero(UInt)), 0, 0)

abstract type AbstractTransform end

struct SetTransform <: AbstractTransform
    name::Symbol
    node::FunSQL.SQLNode
end

funsql_set((name, node) :: Pair) =
    SetTransform(name, node)

struct SnapshotTransform <: AbstractTransform
    names::Vector{Symbol}
end

funsql_snapshot(names...) =
    SnapshotTransform(Symbol[names...])

struct ForgetTransform <: AbstractTransform
    names::Vector{Symbol}
end

funsql_forget(names...) =
    ForgetTransform(Symbol[names...])

struct ForgetAllExceptTransform <: AbstractTransform
    names::Vector{Symbol}
end

funsql_forget_all_except(names...) =
    ForgetAllExceptTransform(Symbol[names...])

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

function run(db, t::AbstractTransform; name::Union{AbstractString, Symbol})
    entries = TransformEntry[]
    schema = TransformSchema()
    for (name, t) in db.catalog
        push!(entries, (t, 0))
        schema[name] = lastindex(entries)
    end
    ctx = TransformContext(Symbol(name), entries, [schema])
    transform!(t, ctx)
    catalog′, ddls = serialize_ddls(db.catalog, ctx)
    for ddl in ddls
        @info ddl
        DBInterface.execute(db, ddl)
    end
    FunSQL.SQLConnection(db.raw, catalog = catalog′)
end

function transform!(t::SetTransform, ctx)
    version = lastindex(ctx.schemas)
    push!(ctx.entries, (t.node, version))
    schema′ = insert(ctx.schemas[end], t.name, lastindex(ctx.entries))
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

function transform!(t::ForgetTransform, ctx)
    schema′ = ctx.schemas[end]
    for name in t.names
        schema′ = delete(schema′, name)
    end
    push!(ctx.schemas, schema′)
    nothing
end

function transform!(t::ForgetAllExceptTransform, ctx)
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

function serialize_ddls(catalog, ctx)
    outs = Set{Tuple{Int, Symbol}}()
    deps_map = Dict{Tuple{Int, Symbol}, Set{Tuple{Int, Symbol}}}()
    queue = Tuple{Int, Symbol}[]
    for (name, index) in ctx.schemas[end]
        key = (index, name)
        push!(outs, key)
        push!(queue, key)
    end
    while !isempty(queue)
        (index, name) = key = popfirst!(queue)
        !haskey(deps_map, key) || continue
        (def, version) = ctx.entries[index]
        deps = Set{Tuple{Int, Symbol}}()
        if def isa FunSQL.SQLNode
            schema = ctx.schemas[version]
            for dep_name in dependencies(def)
                haskey(schema, dep_name) || error(dep_name)
                dep_index = schema[dep_name]
                dep_key = (dep_index, dep_name)
                push!(deps, dep_key)
                push!(queue, dep_key)
            end
        elseif def isa SQLSnapshot
            schema = ctx.schemas[version]
            haskey(schema, def.name) || error(def.name)
            dep_index = schema[def.name]
            dep_key = (dep_index, def.name)
            push!(deps, dep_key)
            push!(queue, dep_key)
        end
        deps_map[key] = deps
    end
    tables = Dict{Symbol, FunSQL.SQLTable}()
    ddls = String[]
    schema_name_sql = FunSQL.render(catalog, FunSQL.ID(ctx.name))
    push!(ddls, "DROP SCHEMA IF EXISTS $schema_name_sql CASCADE")
    push!(ddls, "CREATE SCHEMA $schema_name_sql")
    qualifiers = [ctx.name]
    ctes = FunSQL.SQLNode[]
    subs_map = Dict{Int, Set{Int}}()
    for key in sort(collect(keys(deps_map)))
        (index, name) = key
        (def, version) = ctx.entries[index]
        subs = Set{Int}()
        if def isa FunSQL.SQLTable
            if key in outs
                name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, name))
                sql = FunSQL.render(catalog, FunSQL.From(def))
                ddl = "CREATE VIEW $name_sql AS\n$sql"
                push!(ddls, ddl)
                table = FunSQL.SQLTable(qualifiers = qualifiers, name, columns = sql.columns)
                tables[name] = table
            end
            push!(ctes, name => FunSQL.From(def))
            push!(subs, lastindex(ctes))
        elseif def isa FunSQL.SQLNode
            for (dep_index, _) in deps_map[key]
                union!(subs, subs_map[dep_index])
            end
            if key in outs
                q = def
                for k in sort(collect(subs), rev = true)
                    q = FunSQL.With(ctes[k], over = q)
                end
                name_sql = FunSQL.render(catalog, FunSQL.ID(qualifiers, name))
                sql = FunSQL.render(catalog, q)
                ddl = "CREATE VIEW $name_sql AS\n$sql"
                push!(ddls, ddl)
                table = FunSQL.SQLTable(qualifiers = qualifiers, name, columns = sql.columns)
                tables[name] = table
            end
            push!(ctes, name => def)
            push!(subs, lastindex(ctes))
        elseif def isa SQLSnapshot
            for (dep_index, _) in deps_map[key]
                union!(subs, subs_map[dep_index])
            end
            q = FunSQL.From(def.name)
            for k in sort(collect(subs), rev = true)
                q = FunSQL.With(ctes[k], over = q)
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
            sql = FunSQL.render(catalog, q)
            ddl = "CREATE TABLE $name_sql AS\n$sql"
            push!(ddls, ddl)
            table = FunSQL.SQLTable(qualifiers = qualifiers, snapshot_name, columns = sql.columns)
            tables[snapshot_name] = table
            empty!(subs)
            push!(ctes, name => FunSQL.From(table))
            push!(subs, lastindex(ctes))
        end
        subs_map[index] = subs
    end
    catalog′ = FunSQL.SQLCatalog(tables = tables, dialect = catalog.dialect)
    catalog′, ddls
end

function dependencies(n::FunSQL.SQLNode)
    free = Set{Symbol}()
    bound = Set{Symbol}()
    dependencies!(n, free, bound)
    sort!(collect(free))
end

function dependencies!(n::FunSQL.SQLNode, free, bound)
    dependencies!(n[], free, bound)
end

function dependencies!(ns::Vector{FunSQL.SQLNode}, free, bound)
    for n in ns
        dependencies!(n, free, bound)
    end
end

dependencies!(::Nothing, free, bound) =
    nothing

@generated function dependencies!(n::FunSQL.AbstractSQLNode, free, bound)
    exs = Expr[]
    for f in fieldnames(n)
        t = fieldtype(n, f)
        if t === FunSQL.SQLNode || t === Union{FunSQL.SQLNode, Nothing} || t === Vector{FunSQL.SQLNode}
            ex = quote
                dependencies!(n.$(f), free, bound)
            end
            push!(exs, ex)
        end
    end
    push!(exs, :(return nothing))
    Expr(:block, exs...)
end

function dependencies!(n::FunSQL.FromNode, free, bound)
    source = n.source
    if source isa Symbol && source ∉ bound
        push!(free, source)
    elseif source isa FunSQL.FunctionSource
        dependencies!(source.node, free, bound)
    end
end

function dependencies!(n::Union{FunSQL.WithNode, FunSQL.WithExternalNode}, free, bound)
    dependencies!(n.args, free, bound)
    names = Symbol[name for name in keys(n.label_map) if name ∉ bound]
    for name in names
        push!(bound, name)
    end
    dependencies!(n.over, free, bound)
    for name in names
        pop!(bound, name)
    end
end
