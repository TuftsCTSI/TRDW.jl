struct SQLFormat
    limit::Union{Int, Nothing}

    SQLFormat(; limit = 1000) =
        new(limit)
end

function _format(df, fmt)
    id = "trdw-format-$(rand(UInt64))"
    @htl """
    <div id="$id">
    <table>
    <thead>
    $(_format_thead(df, fmt))
    </thead>
    <tbody>
    $(_format_tbody(df, fmt))
    </tbody>
    </table>
    </div>
    <style>
    $(_format_style(id, df, fmt))
    </style>
    """
end

function _format_thead(df, fmt)
    names = propertynames(df)
    @htl """
    <tr><th></th>$([@htl """<th scope="col">$name</th>""" for name in names])</tr>
    """
end

function _format_tbody(df, fmt)
    (h, w) = size(df)
    if h == 0
        return @htl """
        <tr><td colspan="$(w + 1)" class="trdw-empty"><div>⌀<small>(This table has no rows)</small></div></td></tr>
        """
    end
    limit = fmt.limit
    if limit === nothing || h <= limit + 5
        indexes = collect(1:h)
    else
        indexes = collect(1:limit)
        push!(indexes, 0, h)
    end
    return @htl """$([_format_row(df, i, fmt) for i in indexes])"""
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
    elseif val isa Number
        @htl """<td class="trdw-number">$(sprint(print, val; context = _format_number_context))</td>"""
    else
        @htl """<td>$val</td>"""
    end
end

function _format_style(id, df, fmt)
    return @htl """
    #$id { max-height: 502px; overflow: auto; }
    #$id > table { width: max-content; }
    #$id > table > thead > tr > th { vertical-align; baseline; }
    #$id > table > tbody > tr > th { vertical-align: baseline; }
    #$id > table > tbody > tr > td { max-width: 300px; vertical-align: baseline; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    #$id > table > tbody > tr > td.trdw-number { text-align: right; }
    #$id > table > tbody > tr > td.trdw-empty > div { display: flex; flex-direction: column; align-items: center; font-size: 1.5rem; }
    #$id > table > tbody > tr > td.trdw-empty > div > small { font-size: 0.5rem; }
    #$id > table > tbody > tr:focus > td { overflow: unset; text-overflow: unset; white-space: unset; }
    #$id > table > thead > tr > th { position: sticky; top: -1px; background: var(--main-bg-color); background-clip: padding-box; z-index: 1; }
    #$id > table > thead > tr > th:first-child { position: sticky; left: -10px; background: var(--main-bg-color); background-clip: padding-box; z-index: 2; }
    #$id > table > tbody > tr > th:first-child { position: sticky; left: -10px; background: var(--main-bg-color); background-clip: padding-box; }
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

const funsql_format = FormatNode

FunSQL.Chain(n, n′::FormatNode) =
    FormatNode(FunSQL.Chain(n, n′.over), n′.fmt)

run(db, n::FormatNode) =
    run(db, n.over; fmt = n.fmt)
