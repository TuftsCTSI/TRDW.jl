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
