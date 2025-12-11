# Custom FunSQL nodes

DefineFront(args...; kws...) =
    FunSQL.Define(args...; before = true, kws...)

const funsql_define_front = DefineFront

DefineBefore(args...; name, kws...) =
    FunSQL.Define(args...; before = name, kws...)

const funsql_define_before = DefineBefore

DefineAfter(args...; name, kws...) =
    FunSQL.Define(args...; after = name, kws...)

const funsql_define_after = DefineAfter

mutable struct UndefineNode <: FunSQL.TabularNode
    names::Vector{Symbol}
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function UndefineNode(; names = [], label_map = nothing)
        if label_map !== nothing
            new(names, label_map)
        else
            n = new(names, FunSQL.OrderedDict{Symbol, Int}())
            for (i, name) in enumerate(n.names)
                if name in keys(n.label_map)
                    err = FunSQL.DuplicateLabelError(name, path = FunSQL.SQLQuery[n])
                    throw(err)
                end
                n.label_map[name] = i
            end
            n
        end
    end
end

UndefineNode(names...) =
    UndefineNode(names = Symbol[names...])

const Undefine = FunSQL.SQLQueryCtor{UndefineNode}(:Undefine)

const funsql_undefine = Undefine

function FunSQL.PrettyPrinting.quoteof(n::UndefineNode, ctx::FunSQL.QuoteContext)
    Expr(:call, :Undefine, FunSQL.quoteof(n.names, ctx)...)
end

function FunSQL.resolve(n::UndefineNode, ctx)
    tail′ = FunSQL.resolve(ctx)
    t = FunSQL.row_type(tail′)
    for name in n.names
        ft = get(t.fields, name, FunSQL.EmptyType())
        if ft isa FunSQL.EmptyType
            throw(
                FunSQL.ReferenceError(
                    FunSQL.REFERENCE_ERROR_TYPE.UNDEFINED_NAME,
                    name = name,
                    path = FunSQL.get_path(ctx)))
        end
    end
    fields = FunSQL.FieldTypeMap()
    private_fields = copy(t.private_fields)
    for (f, ft) in t.fields
        if f in keys(n.label_map)
            delete!(private_fields, f)
            continue
        end
        fields[f] = ft
    end
    q′ = FunSQL.Padding(tail = tail′)
    FunSQL.Resolved(FunSQL.RowType(fields, t.group, private_fields), tail = q′)
end

mutable struct TryGetNode <: FunSQL.AbstractSQLNode
    names::Vector{Union{Symbol, Regex}}

    TryGetNode(; names) =
        new(names)
end

TryGetNode(names...) =
    TryGetNode(names = Union{Symbol, Regex}[names...])

const TryGet = FunSQL.SQLQueryCtor{TryGetNode}(:TryGet)

const funsql_try_get = TryGet

function FunSQL.PrettyPrinting.quoteof(n::TryGetNode, ctx::FunSQL.QuoteContext)
    Expr(:call, :TryGet, Any[FunSQL.quoteof(name) for name in n.names]...)
end

function FunSQL.resolve_scalar(n::TryGetNode, ctx)
    if ctx.tail !== nothing
        q′ = FunSQL.unnest(ctx.tail, TryGet(names = n.names), ctx)
        return FunSQL.resolve_scalar(q′, ctx)
    end
    for name in n.names
        if name isa Symbol
            if name in keys(ctx.row_type.fields)
                q′ = FunSQL.Get(name)
                return FunSQL.resolve_scalar(q′, ctx)
            end
        else
            for f in keys(ctx.row_type.fields)
                if occursin(name, String(f))
                    q′ = FunSQL.Get(f)
                    return FunSQL.resolve_scalar(q′, ctx)
                end
            end
        end
    end
    throw(
        FunSQL.ReferenceError(
            FunSQL.REFERENCE_ERROR_TYPE.UNDEFINED_NAME,
            path = FunSQL.get_path(ctx)))
end

mutable struct ExplainConceptIdNode <: FunSQL.AbstractSQLNode
    replace::Bool
    args::Vector{FunSQL.SQLQuery}
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function ExplainConceptIdNode(; replace = false, args = [FunSQL.Get(:concept_name)], label_map = nothing)
        if label_map !== nothing
            new(replace, args, label_map)
        else
            n = new(replace, args, FunSQL.OrderedDict{Symbol, Int}())
            FunSQL.populate_label_map!(n)
            n
        end
    end
end

ExplainConceptIdNode(args...; replace = false) =
    ExplainConceptIdNode(replace = replace, args = FunSQL.SQLQuery[args...])

const ExplainConceptId = FunSQL.SQLQueryCtor{ExplainConceptIdNode}(:ExplainConceptId)

const funsql_explain_concept_id = ExplainConceptId

function FunSQL.PrettyPrinting.quoteof(n::ExplainConceptIdNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, :ExplainConceptId, Any[FunSQL.quoteof(arg, ctx) for arg in n.args]...)
    if n.replace
        push!(ex.args, Expr(:kw, :replace, n.replace))
    end
    ex
end

function FunSQL.resolve(n::ExplainConceptIdNode, ctx)
    tail′ = FunSQL.resolve(ctx)
    t = FunSQL.row_type(tail′)
    q = tail′
    for (f, ft) in t.fields
        s = String(f)
        f === :concept_id || endswith(s, "_concept_id") || continue
        prefix = s[1:end - 10]
        alias = Symbol("$(prefix)concept")
        q = @funsql begin
            $q
            left_join(
                from(concept).define(args = $(n.args)).as($alias),
                $f == $alias.concept_id,
                optional = true,
                private = true)
        end
        defs = [Symbol("$prefix$label") => @funsql($alias.$label) for label in keys(n.label_map)]
        dup_field_aliases = [first(def) for def in defs if haskey(t.fields, first(def))]
        if !isempty(dup_field_aliases)
            q = q |> Undefine(names = dup_field_aliases)
        end
        q = q |> FunSQL.Define(args = defs, after = f, private = (f in t.private_fields))
        if n.replace
            q = q |> Undefine(f)
        end
    end
    FunSQL.resolve(q, ctx)
end

mutable struct SummaryNode <: FunSQL.TabularNode
    names::Vector{Symbol}
    type::Bool
    top_k::Int
    nested::Bool
    private::Bool
    exact::Bool

    SummaryNode(; names = Symbol[], type = true, top_k = 0, nested = false, private = false, exact = false) =
        new(names, type, top_k, nested, private, exact)
end

SummaryNode(names...; type = true, top_k = 0, nested = false, private = false, exact = false) =
    SummaryNode(names = Symbol[names...], type = type, top_k = top_k, nested = nested, private = private, exact = exact)

const Summary = FunSQL.SQLQueryCtor{SummaryNode}(:Summary)

const funsql_summary = Summary
const funsql_density = Summary
const funsql_summarize = Summary

function FunSQL.PrettyPrinting.quoteof(n::SummaryNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, :Summary, FunSQL.quoteof(n.names, ctx)...)
    if n.type
        push!(ex.args, Expr(:kw, :type, n.type))
    end
    if n.top_k > 0
        push!(ex.args, Expr(:kw, :top_k, n.top_k))
    end
    if n.nested
        push!(ex.args, Expr(:kw, :nested, n.nested))
    end
    if n.private
        push!(ex.args, Expr(:kw, :private, n.private))
    end
    if n.exact
        push!(ex.args, Expr(:kw, :exact, n.exact))
    end
    ex
end

function FunSQL.resolve(n::SummaryNode, ctx)
    tail′ = FunSQL.resolve(ctx)
    t = FunSQL.row_type(tail′)
    for name in n.names
        if !haskey(t.fields, name)
            throw(
                FunSQL.ReferenceError(
                    FunSQL.REFERENCE_ERROR_TYPE.UNDEFINED_NAME,
                    name = name,
                    path = FunSQL.get_path(ctx)))
        end
    end
    names = isempty(n.names) ? Set(keys(t.fields)) : Set(n.names)
    cases = _summary_cases(t, names, n.nested || n.private)
    if isempty(cases) && !n.nested
        cases = _summary_cases(t, names, true)
    end
    cols = last.(cases)
    max_i = length(cases)
    args = FunSQL.SQLQuery[]
    push!(args, :column => _summary_switch(first.(cases)))
    if n.type
        push!(args, :type => _summary_switch(map(col -> @funsql(typeof(any_value($col))), cols)))
    end
    push!(
        args,
        :n_not_null => _summary_switch(map(col -> @funsql(count($col)), cols)),
        :pct_not_null => _summary_switch(map(col -> @funsql(floor(100 * count($col) / count(), 1)), cols)))
    if n.exact
        push!(args, :ndv => _summary_switch(map(col -> @funsql(count_distinct($col)), cols)))
    else
        push!(args, :approx_ndv => _summary_switch(map(col -> @funsql(approx_count_distinct($col)), cols)))
    end
    if n.top_k > 0
        for i = 1:n.top_k
            push!(
                args,
                Symbol("approx_top_$i") =>
                    _summary_switch(map(col -> @funsql(_summary_approx_top($col, $(n.top_k), $(i - 1))), cols)))
        end
    end
    q = @funsql begin
        $tail′
        group()
        cross_join(
            summary_case => from(explode(sequence(1, $max_i)), columns = [index]),
            private = true)
        define(args = $args)
    end
    FunSQL.resolve(q, ctx)
end

function _summary_cases(t, name_set, private)
    cases = Tuple{String, FunSQL.SQLQuery}[]
    for (f, ft) in t.fields
        f in name_set || continue
        !(f in t.private_fields) || private || continue
        if ft isa FunSQL.ScalarType
            push!(cases, (String(f), FunSQL.Get(f)))
        elseif ft isa FunSQL.RowType
            subcases = _summary_cases(ft, Set(keys(ft.fields)), private)
            for (n, q) in subcases
                push!(cases, ("$f.$n", FunSQL.Get(f) |> q))
            end
        end
    end
    cases
end

function _summary_switch(branches)
    args = FunSQL.SQLQuery[]
    for (i, branch) in enumerate(branches)
        push!(args, @funsql(summary_case.index == $i), branch)
    end
    FunSQL.Fun.case(args = args)
end

@funsql _summary_approx_top(q, k, i) =
    `[]`(agg(`transform(approx_top_k(?, ?) FILTER (WHERE ? IS NOT NULL), el -> concat(el.item, ' (', floor(100 * el.count / count(?), 1), '%)'))`, $q, $k, $q, $q), $i)

mutable struct CountAllNode <: FunSQL.TabularNode
    include::Union{Regex, Nothing}
    exclude::Union{Regex, Nothing}
    filter::Union{FunSQL.SQLQuery, Nothing}

    CountAllNode(; include = nothing, exclude = nothing, filter = nothing) =
        new(include, exclude, filter)
end

FunSQL.terminal(::Type{CountAllNode}) =
    true

const CountAll = FunSQL.SQLQueryCtor{CountAllNode}(:CountAll)

const funsql_count_all = CountAll

function FunSQL.PrettyPrinting.quoteof(n::CountAllNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, :CountAll)
    if n.include !== nothing
        push!(ex.args, Expr(:kw, :include, FunSQL.quoteof(n.include)))
    end
    if n.exclude !== nothing
        push!(ex.args, Expr(:kw, :exclude, FunSQL.quoteof(n.exclude)))
    end
    if n.filter !== nothing
        push!(ex.args, Expr(:kw, :filter, FunSQL.quoteof(n.filter)))
    end
    ex
end

function FunSQL.resolve(n::CountAllNode, ctx)
    names = sort(collect(keys(ctx.catalog)))
    include = n.include
    if include !== nothing
        names = [name for name in names if occursin(include, String(name))]
    end
    exclude = n.exclude
    if exclude !== nothing
        names = [name for name in names if !occursin(exclude, String(name))]
    end
    filter = n.filter
    args = FunSQL.SQLQuery[]
    for name in names
        arg = @funsql from($name)
        if filter !== nothing
            arg = @funsql $arg.filter($filter)
            try
                arg = FunSQL.resolve(arg, ctx)
            catch e
                e isa FunSQL.ReferenceError || rethrow()
                continue
            end
        end
        arg = @funsql $arg.group().define(name => $(String(name)), n => count())
        push!(args, arg)
    end
    q = @funsql append(args = $args)
    FunSQL.resolve(q, ctx)
end

mutable struct CustomResolveNode <: FunSQL.AbstractSQLNode
    resolve::Any
    resolve_scalar::Any
    terminal::Bool

    CustomResolveNode(; resolve = nothing, resolve_scalar = nothing, terminal = false) =
        new(resolve, resolve_scalar, terminal)
end

FunSQL.terminal(n::CustomResolveNode) =
    n.terminal

CustomResolveNode(resolve; terminal = false) =
    CustomResolveNode(resolve = resolve, terminal = terminal)

const CustomResolve = FunSQL.SQLQueryCtor{CustomResolveNode}(:CustomResolve)

const funsql_custom_resolve = CustomResolve

function FunSQL.PrettyPrinting.quoteof(n::CustomResolveNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(CustomResolve))
    if n.resolve !== nothing
        push!(ex.args, Expr(:kw, :resolve, FunSQL.quoteof(n.resolve)))
    end
    if n.resolve_scalar !== nothing
        push!(ex.args, Expr(:kw, :resolve_scalar, FunSQL.quoteof(n.resolve_scalar)))
    end
    if n.terminal
        push!(ex.args, Expr(:kw, :terminal, n.terminal))
    end
    ex
end

function FunSQL.resolve(n::CustomResolveNode, ctx)
    f = n.resolve
    if f === nothing
        throw(FunSQL.IllFormedError(path = FunSQL.get_path(ctx)))
    end
    FunSQL.resolve(convert(FunSQL.SQLQuery, f(n, ctx)), ctx)
end

function FunSQL.resolve_scalar(n::CustomResolveNode, ctx)
    f = n.resolve_scalar
    if f === nothing
        throw(FunSQL.IllFormedError(path = FunSQL.get_path(ctx)))
    end
    FunSQL.resolve_scalar(convert(FunSQL.SQLQuery, f(n, ctx)), ctx)
end

funsql_if_not_defined(field_name, q) =
    CustomResolve() do n, ctx
        tail′ = FunSQL.resolve(ctx)
        t = FunSQL.row_type(tail′)
        !in(field_name, keys(t.fields)) ? tail′ |> q : tail′
    end

funsql_if_defined(field_name, q, else_q=nothing) =
    CustomResolve() do n, ctx
        tail′ = FunSQL.resolve(ctx)
        t = FunSQL.row_type(tail′)
        in(field_name, keys(t.fields)) ?
            (q !== nothing ? tail′ |> q : tail′) :
            (else_q !== nothing ? tail′ |> else_q : tail′)
    end

funsql_if_defined_scalar(field_name, q, else_q) = begin
    function custom_resolve(n, ctx)
        t = ctx.row_type
        in(field_name, keys(t.fields)) ? q : else_q
    end
    CustomResolve(resolve_scalar = custom_resolve, terminal = true)
end

mutable struct IfSetNode <: FunSQL.AbstractSQLNode
    name::Symbol
    query::Union{FunSQL.SQLQuery, Nothing}
    else_query::Union{FunSQL.SQLQuery, Nothing}

    IfSetNode(; name::Union{AbstractString, Symbol}, query = nothing, else_query = nothing) =
        new(Symbol(name), query, else_query)
end

IfSetNode(name, query, else_query = nothing) =
    IfSetNode(name = name, query = query, else_query = else_query)

const IfSet = FunSQL.SQLQueryCtor{IfSetNode}(:IfSet)

const funsql_if_set = IfSet

function FunSQL.PrettyPrinting.quoteof(n::IfSetNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, :IfSet, QuoteNode(n.name), n.query !== nothing ? FunSQL.quoteof(n.query, ctx) : nothing)
    if n.else_query !== nothing
        push!(ex.args, FunSQL.quoteof(n.else_query, ctx))
    end
    ex
end

function FunSQL.resolve(n::IfSetNode, ctx)
    tail = ctx.tail
    query = haskey(ctx.catalog, n.name) || haskey(ctx.cte_types, n.name) ? n.query : n.else_query
    query = query !== nothing && tail !== nothing ? tail |> query : query !== nothing ? query : tail
    FunSQL.resolve(query, ctx)
end

_concept_attribute(s::String) =
    [s]

_concept_attribute(n::Integer) =
    [string(n)]

_concept_attribute(val) =
    val

mutable struct AssertValidConceptNode <: FunSQL.AbstractSQLNode
    condition::FunSQL.SQLQuery
    ex::Expr

    AssertValidConceptNode(; condition, ex) =
        new(condition, ex)
end

AssertValidConceptNode(condition, ex) =
    AssertValidConceptNode(condition = condition, ex = ex)

FunSQL.terminal(::Type{AssertValidConceptNode}) =
    true

const AssertValidConcept = FunSQL.SQLQueryCtor{AssertValidConceptNode}(:AssertValidConcept)

const funsql_assert_valid_concept = AssertValidConcept

function FunSQL.PrettyPrinting.quoteof(n::AssertValidConceptNode, ctx)
    Expr(:call, :AssertValidConcept, FunSQL.quoteof(n.condition, ctx), Expr(:quote, n.ex))
end

function FunSQL.resolve_scalar(n::AssertValidConceptNode, ctx)
    if !(:concept in keys(ctx.cte_types)) && (:concept in keys(ctx.catalog))
        concept_id = resolve_concept_id(ctx.catalog, n)
        if concept_id !== nothing
            return FunSQL.resolve_scalar(FunSQL.Fun."="(FunSQL.Get.concept_id, FunSQL.Lit(concept_id)), ctx)
        end
    end
    FunSQL.resolve_scalar(n.condition, ctx)
end

function create_concept_cache(conn, t)
    m = t !== nothing ? get_metadata(t) : nothing
    m !== nothing || return
    key = join(string.([t.qualifiers..., t.name]), '.')
    ctime = m.ctime
    scratchname = "$key.$ctime"
    return (conn, @get_scratch!(scratchname))
end

function resolve_concept_id(cat::FunSQL.SQLCatalog, n::AssertValidConceptNode)
    m = get_metadata(cat)
    m !== nothing && m.concept_cache !== nothing || return
    (conn, dir) = m.concept_cache
    db = FunSQL.SQLConnection(conn, catalog = cat)
    q = @funsql concept().filter($(n.condition)).order(concept_id).limit(3)
    sql = FunSQL.render(db, q)
    key = bytes2hex(sha256(sql))
    filename = joinpath(dir, key * ".arrow")
    if isfile(filename)
        df = DataFrame(Arrow.Table(filename))
    else
        df = DataFrame(DBInterface.execute(db, sql))
        ODBC.clear!(conn)
        tmpname = tempname(dir)
        Arrow.write(tmpname, df)
        mv(tmpname, filename, force = true)
    end
    concept_ids = df.concept_id
    invalid_reasons = df.invalid_reason
    if isempty(concept_ids)
        throw(DomainError(n.ex, "concept not found"))
    elseif length(concept_ids) > 1
        choices = join(string.(concept_ids[1:2]), ", ")
        if length(concept_ids) > 2
            choices *= ", …"
        end
        throw(DomainError(n.ex, "concept is ambiguous ($choices)"))
    elseif invalid_reasons[1] !== missing
        throw(DomainError(n.ex, "concept is invalid ($(invalid_reasons[1]))"))
    end
    return concept_ids[1]
end
