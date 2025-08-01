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
    over::Union{FunSQL.SQLNode, Nothing}
    names::Vector{Symbol}
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function UndefineNode(; over = nothing, names = [], label_map = nothing)
        if label_map !== nothing
            new(over, names, label_map)
        else
            n = new(over, names, FunSQL.OrderedDict{Symbol, Int}())
            for (i, name) in enumerate(n.names)
                if name in keys(n.label_map)
                    err = FunSQL.DuplicateLabelError(name, path = [n])
                    throw(err)
                end
                n.label_map[name] = i
            end
            n
        end
    end
end

UndefineNode(names...; over = nothing) =
    UndefineNode(over = over, names = Symbol[names...])

Undefine(args...; kws...) =
    UndefineNode(args...; kws...) |> FunSQL.SQLNode

const funsql_undefine = Undefine

function FunSQL.PrettyPrinting.quoteof(n::UndefineNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(Undefine), FunSQL.quoteof(n.names, ctx)...)
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::UndefineNode, ctx)
    over′ = FunSQL.resolve(n.over, ctx)
    t = FunSQL.row_type(over′)
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
    for (f, ft) in t.fields
        if f in keys(n.label_map)
            continue
        end
        fields[f] = ft
    end
    n′ = FunSQL.Padding(over = over′)
    FunSQL.Resolved(FunSQL.RowType(fields, t.group), over = n′)
    # FIXME: `select(foo => 1).undefine(foo)`
end

mutable struct TryGetNode <: FunSQL.AbstractSQLNode
    over::Union{FunSQL.SQLNode, Nothing}
    names::Vector{Union{Symbol, Regex}}

    TryGetNode(; over = nothing, names) =
        new(over, names)
end

TryGetNode(names...; over = nothing) =
    TryGetNode(over = over, names = Union{Symbol, Regex}[names...])

TryGet(args...; kws...) =
    TryGetNode(args...; kws...) |> FunSQL.SQLNode

const funsql_try_get = TryGet

function FunSQL.PrettyPrinting.quoteof(n::TryGetNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(TryGet), Any[FunSQL.quoteof(name) for name in n.names]...)
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve_scalar(n::TryGetNode, ctx)
    if n.over !== nothing
        n′ = FunSQL.unnest(n.over, TryGet(names = n.names), ctx)
        return FunSQL.resolve_scalar(n′, ctx)
    end
    for name in n.names
        if name isa Symbol
            if name in keys(ctx.row_type.fields)
                n′ = FunSQL.Get(name)
                return FunSQL.resolve_scalar(n′, ctx)
            end
        else
            for f in keys(ctx.row_type.fields)
                if occursin(name, String(f))
                    n′ = FunSQL.Get(f)
                    return FunSQL.resolve_scalar(n′, ctx)
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
    over::Union{FunSQL.SQLNode, Nothing}
    replace::Bool
    args::Vector{FunSQL.SQLNode}
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function ExplainConceptIdNode(; replace = false, over = nothing, args = [FunSQL.Get(:concept_name)], label_map = nothing)
        if label_map !== nothing
            new(over, replace, args, label_map)
        else
            n = new(over, replace, args, FunSQL.OrderedDict{Symbol, Int}())
            FunSQL.populate_label_map!(n)
            n
        end
    end
end

ExplainConceptIdNode(args...; replace = false, over = nothing) =
    ExplainConceptIdNode(replace = replace, over = over, args = FunSQL.SQLNode[args...])

ExplainConceptId(args...; kws...) =
    ExplainConceptIdNode(args...; kws...) |> FunSQL.SQLNode

const funsql_explain_concept_id = ExplainConceptId

function FunSQL.PrettyPrinting.quoteof(n::ExplainConceptIdNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(ExplainConceptId), Any[FunSQL.quoteof(arg, ctx) for arg in n.args]...)
    if n.replace
        push!(ex.args, Expr(:kw, :replace, n.replace))
    end
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::ExplainConceptIdNode, ctx)
    over′ = FunSQL.resolve(n.over, ctx)
    t = FunSQL.row_type(over′)
    q = over′
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
                optional = true)
        end
        defs = [Symbol("$prefix$label") => @funsql($alias.$label) for label in keys(n.label_map)]
        dup_field_aliases = [first(def) for def in defs if haskey(t.fields, first(def))]
        if !isempty(dup_field_aliases)
            q = q |> Undefine(names = dup_field_aliases)
        end
        q = q |> FunSQL.Define(args = defs, after = f)
        if n.replace
            q = q |> Undefine(f)
        end
    end
    FunSQL.resolve(q, ctx)
end

mutable struct SummaryNode <: FunSQL.TabularNode
    over::Union{FunSQL.SQLNode, Nothing}
    names::Vector{Symbol}
    type::Bool
    top_k::Int
    nested::Bool
    exact::Bool

    SummaryNode(; over = nothing, names = Symbol[], type = true, top_k = 0, nested = false, exact = false) =
        new(over, names, type, top_k, nested, exact)
end

SummaryNode(names...; over = nothing, type = true, top_k = 0, nested = false, exact = false) =
    SummaryNode(over = over, names = Symbol[names...], type = type, top_k = top_k, nested = nested, exact = exact)

Summary(args...; kws...) =
    SummaryNode(args...; kws...) |> FunSQL.SQLNode

const funsql_summary = Summary
const funsql_density = Summary

function FunSQL.PrettyPrinting.quoteof(n::SummaryNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(Summary), FunSQL.quoteof(n.names, ctx)...)
    if n.type
        push!(ex.args, Expr(:kw, :type, n.type))
    end
    if n.top_k > 0
        push!(ex.args, Expr(:kw, :top_k, n.top_k))
    end
    if n.nested
        push!(ex.args, Expr(:kw, :nested, n.nested))
    end
    if n.exact
        push!(ex.args, Expr(:kw, :exact, n.exact))
    end
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::SummaryNode, ctx)
    over′ = FunSQL.resolve(n.over, ctx)
    t = FunSQL.row_type(over′)
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
    cases = _summary_cases(t, names, n.nested)
    if isempty(cases) && !n.nested
        cases = _summary_cases(t, names, true)
    end
    cols = last.(cases)
    max_i = length(cases)
    args = FunSQL.SQLNode[]
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
        $over′
        group()
        cross_join(summary_case => from(explode(sequence(1, $max_i)), columns = [index]))
        define(args = $args)
    end
    FunSQL.resolve(q, ctx)
end

function _summary_cases(t, name_set, nested)
    cases = Tuple{String, FunSQL.SQLNode}[]
    for (f, ft) in t.fields
        f in name_set || continue
        if ft isa FunSQL.ScalarType
            push!(cases, (String(f), FunSQL.Get(f)))
        elseif ft isa FunSQL.RowType && nested
            subcases = _summary_cases(ft, Set(keys(ft.fields)), nested)
            for (n, q) in subcases
                push!(cases, ("$f.$n", FunSQL.Get(f) |> q))
            end
        end
    end
    cases
end

function _summary_switch(branches)
    args = FunSQL.SQLNode[]
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
    filter::Union{FunSQL.SQLNode, Nothing}

    CountAllNode(; include = nothing, exclude = nothing, filter = nothing) =
        new(include, exclude, filter)
end

CountAll(args...; kws...) =
    CountAllNode(args...; kws...) |> FunSQL.SQLNode

const funsql_count_all = CountAll

function FunSQL.PrettyPrinting.quoteof(n::CountAllNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(CountAll))
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
    args = FunSQL.SQLNode[]
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
    over::Union{FunSQL.SQLNode, Nothing}
    resolve::Any
    resolve_scalar::Any
    terminal::Bool

    CustomResolveNode(; over = nothing, resolve = nothing, resolve_scalar = nothing, terminal = false) =
        new(over, resolve, resolve_scalar, terminal)
end

CustomResolveNode(resolve; over = nothing, terminal = false) =
    CustomResolveNode(over = over, resolve = resolve, terminal = terminal)

CustomResolve(args...; kws...) =
    CustomResolveNode(args...; kws...) |> FunSQL.SQLNode

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
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.rebase(n::CustomResolveNode, n′)
    if n.terminal
        throw(FunSQL.RebaseError(path = [n]))
    end
    CustomResolveNode(
        over = FunSQL.rebase(n.over, n′),
        resolve = n.resolve,
        resolve_scalar = n.resolve_scalar,
        terminal = n.terminal)
end

function FunSQL.resolve(n::CustomResolveNode, ctx)
    f = n.resolve
    if f === nothing
        throw(FunSQL.IllFormedError(path = FunSQL.get_path(ctx)))
    end
    FunSQL.resolve(convert(FunSQL.SQLNode, f(n, ctx)), ctx)
end

function FunSQL.resolve_scalar(n::CustomResolveNode, ctx)
    f = n.resolve_scalar
    if f === nothing
        throw(FunSQL.IllFormedError(path = FunSQL.get_path(ctx)))
    end
    FunSQL.resolve_scalar(convert(FunSQL.SQLNode, f(n, ctx)), ctx)
end

funsql_if_not_defined(field_name, q) =
    CustomResolve() do n, ctx
        over′ = FunSQL.resolve(n.over, ctx)
        t = FunSQL.row_type(over′)
        !in(field_name, keys(t.fields)) ? over′ |> q : over′
    end

funsql_if_defined(field_name, q, else_q=nothing) =
    CustomResolve() do n, ctx
        over′ = FunSQL.resolve(n.over, ctx)
        t = FunSQL.row_type(over′)
        in(field_name, keys(t.fields)) ?
            (q !== nothing ? over′ |> q : over′) :
            (else_q !== nothing ? over′ |> else_q : over′)
    end

funsql_if_defined_scalar(field_name, q, else_q) = begin
    function custom_resolve(n, ctx)
        t = ctx.row_type
        in(field_name, keys(t.fields)) ? q : else_q
    end
    CustomResolve(resolve_scalar = custom_resolve, terminal = true)
end

mutable struct IfSetNode <: FunSQL.AbstractSQLNode
    over::Union{FunSQL.SQLNode, Nothing}
    name::Symbol
    node::Union{FunSQL.SQLNode, Nothing}
    else_node::Union{FunSQL.SQLNode, Nothing}

    IfSetNode(; over = nothing, name::Union{AbstractString, Symbol}, node = nothing, else_node = nothing) =
        new(over, Symbol(name), node, else_node)
end

IfSetNode(name, node, else_node = nothing; over = nothing) =
    IfSetNode(over = over, name = name, node = node, else_node = else_node)

IfSet(args...; kws...) =
    IfSetNode(args...; kws...) |> FunSQL.SQLNode

const funsql_if_set = IfSet

function FunSQL.PrettyPrinting.quoteof(n::IfSetNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(IfSet), QuoteNode(n.name), n.node !== nothing ? FunSQL.quoteof(n.node, ctx) : nothing)
    if n.else_node !== nothing
        push!(ex.args, FunSQL.quoteof(n.else_node, ctx))
    end
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::IfSetNode, ctx)
    over = n.over
    node = haskey(ctx.catalog, n.name) || haskey(ctx.cte_types, n.name) ? n.node : n.else_node
    node = node !== nothing && over !== nothing ? over |> node : node !== nothing ? node : over
    FunSQL.resolve(node, ctx)
end

_concept_attribute(s::String) =
    [s]

_concept_attribute(n::Integer) =
    [string(n)]

_concept_attribute(val) =
    val

mutable struct AssertValidConceptNode <: FunSQL.AbstractSQLNode
    condition::FunSQL.SQLNode
    ex::Expr

    AssertValidConceptNode(; condition, ex) =
        new(condition, ex)
end

AssertValidConceptNode(condition, ex) =
    AssertValidConceptNode(condition = condition, ex = ex)

AssertValidConcept(args...; kws...) =
    AssertValidConceptNode(args...; kws...) |> FunSQL.SQLNode

const funsql_assert_valid_concept = AssertValidConcept

function FunSQL.PrettyPrinting.quoteof(n::AssertValidConceptNode, ctx)
    Expr(:call, nameof(AssertValidConcept), FunSQL.quoteof(n.condition, ctx), Expr(:quote, n.ex))
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
    q = @funsql concept($(n.condition)).order(concept_id).limit(3)
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
