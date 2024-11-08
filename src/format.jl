struct Roundup{T<:Number}
    val::T

    Roundup{T}(val) where {T<:Number} =
        new{T}(val)
end

Roundup(val) =
    val

Roundup(val::T) where {T<:Number} =
    Roundup{T}(val)

Base.show(io::IO, r::Roundup) =
    0 <= r.val <= 10 ? print(io, "≤10") : show(io, r.val)

struct SQLFormat
    caption::Union{String, Nothing}
    limit::Union{Int, Nothing}
    group_by::Union{Symbol, Nothing}
    group_limit::Union{Int, Nothing}
    roundup::Union{Vector{Symbol}, Nothing}
    hide_null_cols::Bool
    scroll::Bool

    SQLFormat(; caption = nothing, limit = 1000, group_by = nothing, group_limit = nothing, roundup = nothing, hide_null_cols = false, scroll = true) =
        new(caption, limit, group_by, group_limit, roundup, hide_null_cols, scroll)
end

function _format(df, fmt)
    if fmt.hide_null_cols
        df = df[!, any.(!ismissing, eachcol(df))]
    end
    id = "trdw-format-$(rand(UInt64))"
    @htl """
    <div id="$id">
    <table>
    $(_format_caption(df, fmt))
    $(_format_thead(df, fmt))
    $(_format_tbody(df, fmt))
    </table>
    </div>
    $(_format_style(id, df, fmt))
    """
end

function _format_caption(df, fmt)
    fmt.caption !== nothing || return
    @htl """
    <caption>$(fmt.caption)</caption>
    """
end

function _format_thead(df, fmt)
    names = propertynames(df)
    if fmt.group_by !== nothing
        fmt.group_by in names || throw(DomainError(fmt.group_by, "missing grouping column"))
        filter!(!=(fmt.group_by), names)
    end
    @htl """
    <thead>
    <tr><th></th>$([@htl """<th scope="col">$name</th>""" for name in names])</tr>
    </thead>
    """
end

function _format_tbody(df, fmt)
    (h, w) = size(df)
    if h == 0
        if fmt.group_by === nothing
            w += 1
        end
        return @htl """
        <tbody>
        <tr><td colspan="$w" class="trdw-empty"><div>⌀<small>(This table has no rows)</small></div></td></tr>
        </tbody>
        """
    end
    roundup = fmt.roundup
    if roundup !== nothing
        for n in propertynames(df)
            n ∈ roundup || continue
            df = transform(df, n => ByRow(Roundup) => n)
        end
    end
    if fmt.group_by !== nothing
        gdf = groupby(df, fmt.group_by, sort = false)
        df = df[!, Not(fmt.group_by)]
        limit = fmt.limit
        if limit === nothing || h <= limit + 5
            n = length(gdf)
        else
            n = 1
            l = size(gdf[1], 1)
            l = min(l, something(fmt.group_limit, l))
            while n < length(gdf)
                l′ = size(gdf[n + 1], 1)
                l′ = min(l′, something(fmt.group_limit, l′))
                l += l′
                l <= limit || break
                n += 1
            end
        end
        return @htl """
        $([_format_group(gdf[k], fmt) for k = 1:n])
        $(n < length(gdf) ? @htl("""<tbody>$(_format_row(df, 0, fmt))</tbody>""") : "")
        """
    end
    @htl """
    <tbody>
    $(_format_rows(df, fmt))
    </tbody>
    """
end

function _format_group(df, fmt)
    group = df[1, fmt.group_by]
    df = df[!, Not(fmt.group_by)]
    w = size(df, 2)
    @htl """
    <tbody>
    <tr><th scope="colgroup" colspan="$(w + 1)" class="trdw-group"><div>$group</div></th></tr>
    $(_format_rows(df, fmt))
    </tbody>
    """
end

function _format_rows(df, fmt)
    (h, w) = size(df)
    limit = fmt.limit
    if fmt.group_by !== nothing && fmt.group_limit !== nothing
        limit = fmt.group_limit
    end
    if limit === nothing || h <= limit + 5
        indexes = collect(1:h)
    else
        indexes = collect(1:limit)
        push!(indexes, 0, h)
    end
    @htl """$([_format_row(df, i, fmt) for i in indexes])"""
end

function _format_row(df, i, fmt)
    w = size(df, 2)
    if i == 0
        return @htl """
        <tr><th class="trdw-vdots">⋮</td>$(w > 0 ? @htl("""<td colspan="$w"></td>""") : "")</tr>
        """
    end
    return @htl """
    <tr tabindex="-1"><th scope="row">$i</th>$([_format_cell(df[i, j], fmt) for j = 1:w])</tr>
    """
end

const _format_number_context = :compact => true

function _format_cell(val, fmt)
    if val === missing
        @htl """<td class="trdw-missing"></td>"""
    elseif val isa Union{Number, Roundup}
        @htl """<td class="trdw-number">$(sprint(print, val; context = _format_number_context))</td>"""
    else
        @htl """<td>$val</td>"""
    end
end

function _format_style(id, df, fmt)
    return @htl """
    <style>
    $(fmt.scroll ? @htl("#$id { max-height: 502px; overflow: auto; }") : "")
    #$id > table { width: max-content; }
    #$id > table > caption { padding: .2rem .5rem; }
    #$id > table > thead > tr > th { vertical-align; baseline; }
    #$id > table > tbody > tr:first-child > th { border-top: 1px solid var(--table-border-color); }
    #$id > table > tbody > tr:first-child > td { border-top: 1px solid var(--table-border-color); }
    #$id > table > tbody > tr > th { vertical-align: baseline; }
    #$id > table > tbody > tr > td { max-width: 300px; vertical-align: baseline; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    #$id > table > tbody > tr > td.trdw-number { text-align: right; }
    #$id > table > tbody > tr > td.trdw-empty > div { display: flex; flex-direction: column; align-items: center; font-size: 1.5rem; }
    #$id > table > tbody > tr > td.trdw-empty > div > small { font-size: 0.5rem; }
    #$id > table > tbody > tr:focus > td { overflow: unset; text-overflow: unset; white-space: unset; }
    #$id > table > thead > tr > th { position: sticky; top: -1px; background: var(--main-bg-color); background-clip: padding-box; z-index: 1; }
    #$id > table > thead > tr > th:first-child { position: sticky; left: -10px; background: var(--main-bg-color); background-clip: padding-box; z-index: 2; }
    #$id > table > tbody > tr > th:first-child { position: sticky; left: -10px; background: var(--main-bg-color); background-clip: padding-box; }
    #$id > table > tbody > tr > th.trdw-group { top: 24px; text-align: left; z-index: 2; }
    #$id > table > tbody > tr > th.trdw-group > div { display: inline-block; position: sticky; left: 0; }
    </style>
    """
end

struct FormatNode
    over::FunSQL.SQLNode
    fmt::SQLFormat

    FormatNode(over, fmt) =
        new(over, fmt)
end

FormatNode(; kws...) =
    FormatNode(FunSQL.Define(), SQLFormat(; kws...))

FormatNode(caption; kws...) =
    FormatNode(; caption, kws...)

const funsql_format = FormatNode

FunSQL.Chain(n, n′::FormatNode) =
    FormatNode(FunSQL.Chain(n, n′.over), n′.fmt)

run(db, n::FormatNode) =
    run(db, n.over; fmt = n.fmt)
