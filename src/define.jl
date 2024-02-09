mutable struct DefineFrontNode <: FunSQL.TabularNode
    over::Union{FunSQL.SQLNode, Nothing}
    args::Vector{FunSQL.SQLNode}
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function DefineFrontNode(; over = nothing, args = [], label_map = nothing)
        if label_map !== nothing
            new(over, args, label_map)
        else
            n = new(over, args, FunSQL.OrderedDict{Symbol, Int}())
            FunSQL.populate_label_map!(n)
            n
        end
    end
end

DefineFrontNode(args...; over = nothing) =
    DefineFrontNode(over = over, args = FunSQL.SQLNode[args...])

DefineFront(args...; kws...) =
    DefineFrontNode(args...; kws...) |> FunSQL.SQLNode

const funsql_define_front = DefineFront

function FunSQL.PrettyPrinting.quoteof(n::DefineFrontNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(DefineFront), FunSQL.quoteof(n.args, ctx)...)
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::DefineFrontNode, ctx)
    over′ = FunSQL.resolve(n.over, ctx)
    t = FunSQL.row_type(over′)
    args′ = FunSQL.resolve_scalar(n.args, ctx, t)
    fields = FunSQL.FieldTypeMap()
    for (f, i) in n.label_map
        if !haskey(t.fields, f)
            fields[f] = FunSQL.type(args′[i])
        end
    end
    for (f, ft) in t.fields
        i = get(n.label_map, f, nothing)
        if i !== nothing
            ft = FunSQL.type(args′[i])
        end
        fields[f] = ft
    end
    n′ = FunSQL.Define(over = over′, args = args′, label_map = n.label_map)
    FunSQL.Resolved(FunSQL.RowType(fields, t.group), over = n′)
end

mutable struct DefineBeforeNode <: FunSQL.TabularNode
    over::Union{FunSQL.SQLNode, Nothing}
    args::Vector{FunSQL.SQLNode}
    name::Symbol
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function DefineBeforeNode(; over = nothing, args = [], name, label_map = nothing)
        if label_map !== nothing
            new(over, args, name, label_map)
        else
            n = new(over, args, name, FunSQL.OrderedDict{Symbol, Int}())
            FunSQL.populate_label_map!(n)
            n
        end
    end
end

DefineBeforeNode(args...; over = nothing, name) =
    DefineBeforeNode(over = over, args = FunSQL.SQLNode[args...], name = name)

DefineBefore(args...; kws...) =
    DefineBeforeNode(args...; kws...) |> FunSQL.SQLNode

const funsql_define_before = DefineBefore

function FunSQL.PrettyPrinting.quoteof(n::DefineBeforeNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(DefineBefore), FunSQL.quoteof(n.args, ctx)...)
    push!(ex.args, Expr(:kw, :name, QuoteNode(n.name)))
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::DefineBeforeNode, ctx)
    over′ = FunSQL.resolve(n.over, ctx)
    t = FunSQL.row_type(over′)
    if !haskey(t.fields, n.name)
        throw(
            FunSQL.ReferenceError(
                    FunSQL.REFERENCE_ERROR_TYPE.UNDEFINED_NAME,
                    name = n.name,
                    path = FunSQL.get_path(ctx)))
    end
    args′ = FunSQL.resolve_scalar(n.args, ctx, t)
    fields = FunSQL.FieldTypeMap()
    for (f, ft) in t.fields
        if f === n.name
            for (l, i) in n.label_map
                if !haskey(t.fields, l)
                    fields[l] = FunSQL.type(args′[i])
                end
            end
        end
        i = get(n.label_map, f, nothing)
        if i !== nothing
            ft = FunSQL.type(args′[i])
        end
        fields[f] = ft
    end
    n′ = FunSQL.Define(over = over′, args = args′, label_map = n.label_map)
    FunSQL.Resolved(FunSQL.RowType(fields, t.group), over = n′)
end

mutable struct DefineAfterNode <: FunSQL.TabularNode
    over::Union{FunSQL.SQLNode, Nothing}
    args::Vector{FunSQL.SQLNode}
    name::Symbol
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function DefineAfterNode(; over = nothing, args = [], name, label_map = nothing)
        if label_map !== nothing
            new(over, args, name, label_map)
        else
            n = new(over, args, name, FunSQL.OrderedDict{Symbol, Int}())
            FunSQL.populate_label_map!(n)
            n
        end
    end
end

DefineAfterNode(args...; over = nothing, name) =
    DefineAfterNode(over = over, args = FunSQL.SQLNode[args...], name = name)

DefineAfter(args...; kws...) =
    DefineAfterNode(args...; kws...) |> FunSQL.SQLNode

const funsql_define_after = DefineAfter

function FunSQL.PrettyPrinting.quoteof(n::DefineAfterNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(DefineAfter), FunSQL.quoteof(n.args, ctx)...)
    push!(ex.args, Expr(:kw, :name, QuoteNode(n.name)))
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::DefineAfterNode, ctx)
    over′ = FunSQL.resolve(n.over, ctx)
    t = FunSQL.row_type(over′)
    if !haskey(t.fields, n.name)
        throw(
            FunSQL.ReferenceError(
                    FunSQL.REFERENCE_ERROR_TYPE.UNDEFINED_NAME,
                    name = n.name,
                    path = FunSQL.get_path(ctx)))
    end
    args′ = FunSQL.resolve_scalar(n.args, ctx, t)
    fields = FunSQL.FieldTypeMap()
    for (f, ft) in t.fields
        i = get(n.label_map, f, nothing)
        if i !== nothing
            ft = FunSQL.type(args′[i])
        end
        fields[f] = ft
        if f === n.name
            for (l, i) in n.label_map
                if !haskey(t.fields, l)
                    fields[l] = FunSQL.type(args′[i])
                end
            end
        end
    end
    n′ = FunSQL.Define(over = over′, args = args′, label_map = n.label_map)
    FunSQL.Resolved(FunSQL.RowType(fields, t.group), over = n′)
end

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
    n′ = FunSQL.IntAutoDefine(over = over′)
    FunSQL.Resolved(FunSQL.RowType(fields), over = n′)
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
        n′ = FunSQL.rebind(n.over, TryGet(names = n.names), ctx)
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
    args::Vector{FunSQL.SQLNode}
    label_map::FunSQL.OrderedDict{Symbol, Int}

    function ExplainConceptIdNode(; over = nothing, args = [FunSQL.Get(:concept_name)], label_map = nothing)
        if label_map !== nothing
            new(over, args, label_map)
        else
            n = new(over, args, FunSQL.OrderedDict{Symbol, Int}())
            FunSQL.populate_label_map!(n)
            n
        end
    end
end

ExplainConceptIdNode(args...; over = nothing) =
    ExplainConceptIdNode(over = over, args = FunSQL.SQLNode[args...])

ExplainConceptId(args...; kws...) =
    ExplainConceptIdNode(args...; kws...) |> FunSQL.SQLNode

const funsql_explain_concept_id = ExplainConceptId

function FunSQL.PrettyPrinting.quoteof(n::ExplainConceptIdNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(ExplainConceptId), Any[FunSQL.quoteof(arg, ctx) for arg in n.args]...)
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
        q = q |> DefineAfter(args = defs, name = f)
    end
    FunSQL.resolve(q, ctx)
end

mutable struct DensityNode <: FunSQL.TabularNode
    over::Union{FunSQL.SQLNode, Nothing}
    names::Vector{Symbol}
    top_k::Int
    nested::Bool

    DensityNode(; over = nothing, names = Symbol[], top_k = 0, nested = false) =
        new(over, names, top_k, nested)
end

DensityNode(names...; over = nothing, top_k = 0, nested = false) =
    DensityNode(over = over, names = Symbol[names...], top_k = top_k, nested = nested)

Density(args...; kws...) =
    DensityNode(args...; kws...) |> FunSQL.SQLNode

const funsql_density = Density

function FunSQL.PrettyPrinting.quoteof(n::DensityNode, ctx::FunSQL.QuoteContext)
    ex = Expr(:call, nameof(Density), FunSQL.quoteof(n.names, ctx)...)
    if n.top_k > 0
        push!(ex.args, Expr(:kw, :top_k, n.top_k))
    end
    if n.nested
        push!(ex.args, Expr(:kw, :nested, n.nested))
    end
    if n.over !== nothing
        ex = Expr(:call, :|>, FunSQL.quoteof(n.over, ctx), ex)
    end
    ex
end

function FunSQL.resolve(n::DensityNode, ctx)
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
    cases = _density_cases(t, isempty(n.names) ? Set(keys(t.fields)) : Set(n.names), n.nested)
    cols = last.(cases)
    max_i = length(cases)
    args =FunSQL.SQLNode[]
    push!(
        args,
        :name => _density_switch(first.(cases)),
        :n_not_null => _density_switch(map(col -> @funsql(count($col)), cols)),
        :pct_not_null => _density_switch(map(col -> @funsql(100 * count($col) / count()), cols)),
        :approx_n_distinct => _density_switch(map(col -> @funsql(approx_count_distinct($col)), cols)))
    if n.top_k > 0
        push!(
            args,
            :approx_top_val =>
                _density_switch(map(col -> @funsql(approx_top_k_val($col, $(n.top_k))), cols)),
            :approx_top_pct =>
                _density_switch(map(col -> @funsql(approx_top_k_pct($col, $(n.top_k))), cols)))
    end
    q = @funsql begin
        $over′
        group()
        cross_join(density_case => from(explode(sequence(1, $max_i)), columns = [index]))
        define(args = $args)
    end
    FunSQL.resolve(q, ctx)
end

function _density_cases(t, name_set, nested)
    cases = Tuple{String, FunSQL.SQLNode}[]
    for (f, ft) in t.fields
        f in name_set || continue
        if ft isa FunSQL.ScalarType
            push!(cases, (String(f), FunSQL.Get(f)))
        elseif ft isa FunSQL.RowType && nested
            subcases = _density_cases(ft, Set(keys(ft.fields)), nested)
            for (n, q) in subcases
                push!(cases, ("$f.$n", FunSQL.Get(f) |> q))
            end
        end
    end
    cases
end

function _density_switch(branches)
    args = FunSQL.SQLNode[]
    for (i, branch) in enumerate(branches)
        push!(args, @funsql(density_case.index == $i), branch)
    end
    FunSQL.Fun.case(args = args)
end

@funsql approx_top_k_val(q, k = 5) =
    maybe_first_only(fun(`transform(?, val -> string(val))`, approx_top_k($q, $k, filter = is_not_null($q)) >> item), $k)

@funsql approx_top_k_pct(q, k = 5) =
    maybe_first_only(fun(`transform(?, k -> round(100 * k / ?, 1))`, approx_top_k($q, $k, filter = is_not_null($q)) >> count, count($q)), $k)

funsql_maybe_first_only(q, k) =
    k == 1 ? FunSQL.Fun."?[0]"(q) : q

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
    names = sort(collect(keys(ctx.tables)))
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
