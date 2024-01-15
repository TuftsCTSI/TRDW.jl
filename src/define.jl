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

FunSQL.label(n::UndefineNode) =
    FunSQL.label(n.over)

FunSQL.rebase(n::UndefineNode, n′) =
    UndefineNode(over = FunSQL.rebase(n.over, n′), names = n.names, label_map = n.label_map)

function FunSQL.annotate(n::UndefineNode, ctx)
    over′ = FunSQL.annotate(n.over, ctx)
    Undefine(over = over′, names = n.names, label_map = n.label_map)
end

function FunSQL.resolve(n::UndefineNode, ctx)
    t = FunSQL.box_type(n.over)
    for name in n.names
        ft = get(t.row.fields, name, FunSQL.EmptyType())
        if ft isa FunSQL.EmptyType
            throw(
                FunSQL.ReferenceError(
                    FunSQL.REFERENCE_ERROR_TYPE.UNDEFINED_NAME,
                    name = name,
                    path = FunSQL.get_path(ctx, convert(FunSQL.SQLNode, n))))
        end
    end
    fields = FunSQL.FieldTypeMap()
    for (f, ft) in t.row.fields
        if f in keys(n.label_map)
            continue
        end
        fields[f] = ft
    end
    row = FunSQL.RowType(fields, t.row.group)
    FunSQL.BoxType(t.name, row, t.handle_map)
end

function FunSQL.link!(n::UndefineNode, refs::Vector{FunSQL.SQLNode}, ctx)
    box = n.over[]::FunSQL.BoxNode
    append!(box.refs, refs)
end

function FunSQL.assemble(n::UndefineNode, refs, ctx)
    FunSQL.assemble(n.over, ctx)
    # FIXME: `select(foo => 1).undefine(foo)`
end
