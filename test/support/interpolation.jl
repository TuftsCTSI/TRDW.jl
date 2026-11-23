# A static check for parameters used without `$` inside `@funsql`.

struct UninterpolatedName
    name::Symbol
    line::Int
end

struct FunSQLDefinition
    file::String
    line::Int
    name::Symbol
    uses::Vector{UninterpolatedName}
end

const JOIN_CALLS = (:join, :left_join, :cross_join, :with)

macro_name(m::Symbol) = m
macro_name(m::GlobalRef) = m.name
macro_name(m::QuoteNode) = m.value
macro_name(m::Expr) = m.head == :. ? macro_name(m.args[end]) : nothing
macro_name(m) = nothing

is_macrocall(ex, name) =
    ex isa Expr && ex.head == :macrocall && macro_name(ex.args[1]) === Symbol(name)

is_funsql(ex) = is_macrocall(ex, "@funsql")
is_doc(ex) = is_macrocall(ex, "@doc")

contains_funsql(ex) =
    is_funsql(ex) || (ex isa Expr && any(contains_funsql, ex.args))

# The name a parameter expression binds: `x`, `x::T`, `x = default`,
# `x...`, `$x`.
function param_name(p)
    p isa Symbol && return p
    p isa Expr && p.head in (:kw, :(::), :..., :$) && return param_name(p.args[1])
    nothing
end

# The function name, parameter names and default expressions of a
# signature such as `f(a, b = 1; c, d = 2)`.
function signature(call)
    while call isa Expr && call.head in (:where, :(::))
        call = call.args[1]
    end
    call isa Expr && call.head == :call || return nothing
    names = Symbol[]
    defaults = Any[]
    function add!(p)
        if p isa Expr && p.head == :(::) && p.args[1] isa Expr && p.args[1].head == :tuple
            # a destructured argument: `(prefix, query)::Pair`
            append!(names, filter(x -> x isa Symbol, p.args[1].args))
            return
        end
        n = param_name(p)
        n === nothing || push!(names, n)
        p isa Expr && p.head == :kw && push!(defaults, p.args[2])
    end
    for a in call.args[2:end]
        if a isa Expr && a.head == :parameters
            foreach(add!, a.args)
        else
            add!(a)
        end
    end
    (; name = call.args[1], names, defaults)
end

mutable struct Scan
    line::Int
    uses::Vector{UninterpolatedName}
end

report!(s::Scan, name) = push!(s.uses, UninterpolatedName(name, s.line))

# Walk `ex` as `@funsql` code.
function scan_funsql!(s::Scan, ex, names)
    if ex isa Symbol
        ex in names && report!(s, ex)
    elseif ex isa LineNumberNode
        s.line = ex.line
    elseif ex isa Expr
        h = ex.head
        if h == :$
            scan_julia!(s, ex.args[1], names)
        elseif h in (:kw, :(=))
            # `name = value` names a keyword or a named-tuple field
            scan_funsql!(s, ex.args[2], names)
        elseif h == :call && ex.args[1] == :(=>) && length(ex.args) == 3
            # `name => value`: a bare `name` names the output, it's not a use
            ex.args[2] isa Symbol || scan_funsql!(s, ex.args[2], names)
            scan_funsql!(s, ex.args[3], names)
        elseif h == :call
            # args[1] is the function
            interpolated = Set(
                a.args[1] for a in ex.args[2:end]
                if a isa Expr && a.head == :$ && a.args[1] isa Symbol)
            for a in ex.args[2:end]
                a isa Symbol && a in interpolated && continue
                scan_funsql!(s, a, names)
            end
        elseif h == :.
            scan_funsql!(s, ex.args[1], names)
            field = ex.args[2]
            if field isa QuoteNode
                field.value in names && report!(s, field.value)
            else
                scan_funsql!(s, field, names)
            end
        elseif h == :macrocall
            foreach(a -> scan_funsql!(s, a, names), ex.args[2:end])
        elseif h != :quote
            foreach(a -> scan_funsql!(s, a, names), ex.args)
        end
    end
    s
end

# Walk `ex` as Julia code, checking the `@funsql` code inside it. The loop
# variables of a comprehension count as names inside it.
function scan_julia!(s::Scan, ex, names)
    if ex isa LineNumberNode
        s.line = ex.line
    elseif is_funsql(ex)
        foreach(a -> scan_funsql!(s, a, names), ex.args[2:end])
    elseif ex isa Expr && ex.head == :generator
        inner = union(names, loop_variables(ex.args[2:end]))
        scan_julia!(s, ex.args[1], inner)
        foreach(a -> scan_julia!(s, a, names), ex.args[2:end])
    elseif ex isa Expr
        foreach(a -> scan_julia!(s, a, names), ex.args)
    end
    s
end

function loop_variables(iterators)
    vars = Symbol[]
    for it in iterators
        it isa Expr && it.head == :filter && (append!(vars, loop_variables(it.args[2:end])); continue)
        it isa Expr && it.head == :(=) || continue
        v = it.args[1]
        v isa Symbol && push!(vars, v)
        v isa Expr && v.head == :tuple && append!(vars, filter(x -> x isa Symbol, v.args))
    end
    vars
end

# Names that `@funsql` code binds as subquery aliases.
function aliases!(out, ex)
    ex isa Expr || return out
    ex.head == :$ && return out
    if ex.head == :call && ex.args[1] in (:into, :as) && length(ex.args) >= 2 && ex.args[2] isa Symbol
        push!(out, ex.args[2])
    elseif ex.head == :call && ex.args[1] in JOIN_CALLS
        for a in ex.args[2:end]
            if a isa Expr && a.head == :call && a.args[1] == :(=>) && a.args[2] isa Symbol
                push!(out, a.args[2])
            end
        end
    end
    foreach(a -> aliases!(out, a), ex.args)
    out
end

# Names that Julia code binds: assignments, loop variables, and the
# parameters of inner functions. `@funsql` code isn't Julia code.
function julia_locals!(out, ex)
    ex isa Expr || return out
    is_funsql(ex) && return out
    if ex.head == :(=) && ex.args[1] isa Symbol
        push!(out, ex.args[1])
    elseif ex.head == :(=) && ex.args[1] isa Expr && ex.args[1].head == :tuple
        append!(out, filter(x -> x isa Symbol, ex.args[1].args))
    elseif ex.head == :for
        append!(out, loop_variables(ex.args[1].head == :block ? ex.args[1].args : [ex.args[1]]))
    elseif ex.head == :function || (ex.head == :(=) && ex.args[1] isa Expr && ex.args[1].head == :call)
        sig = signature(ex.args[1])
        sig === nothing || append!(out, sig.names)
    elseif ex.head == :->
        params = ex.args[1]
        params isa Symbol && push!(out, params)
        params isa Expr && params.head == :tuple && append!(out, filter(x -> x isa Symbol, map(param_name, params.args)))
    end
    foreach(a -> julia_locals!(out, a), ex.args)
    out
end

# `name(params) = body` written inside `@funsql`.
function check_funsql_definition(def, file, line)
    def isa Expr && def.head == :(=) || return nothing
    sig = signature(def.args[1])
    sig === nothing && return nothing
    names = setdiff(sig.names, aliases!(Symbol[], def.args[2]))
    # The body of `f(x) = …` starts with the line of the definition itself,
    # which a docstring before it would otherwise hide.
    body = def.args[2]
    if body isa Expr && body.head == :block && !isempty(body.args) && body.args[1] isa LineNumberNode
        line = body.args[1].line
    end
    s = Scan(line, UninterpolatedName[])
    foreach(d -> scan_funsql!(s, d, names), sig.defaults)
    scan_funsql!(s, def.args[2], names)
    FunSQLDefinition(file, line, Symbol(sig.name), s.uses)
end

# A Julia function that builds queries with `@funsql`.
function check_julia_definition(def, file, line)
    def isa Expr && def.head in (:function, :(=)) && length(def.args) >= 2 || return nothing
    sig = signature(def.args[1])
    sig === nothing && return nothing
    contains_funsql(def.args[2]) || return nothing
    names = unique!(julia_locals!(copy(sig.names), def.args[2]))
    s = Scan(line, UninterpolatedName[])
    scan_julia!(s, def.args[2], names)
    FunSQLDefinition(file, line, Symbol(sig.name), s.uses)
end

function collect_definitions!(defs, ex, file, line)
    if ex isa LineNumberNode
        line[] = ex.line
    elseif is_doc(ex)
        foreach(a -> collect_definitions!(defs, a, file, line), ex.args[2:end])
    elseif is_funsql(ex)
        for a in ex.args[2:end]
            if a isa LineNumberNode
                line[] = a.line
            elseif a isa Expr && a.head == :block
                for b in a.args
                    if b isa LineNumberNode
                        line[] = b.line
                    elseif is_doc(b)
                        for c in b.args[2:end]
                            c isa LineNumberNode && (line[] = c.line; continue)
                            d = check_funsql_definition(c, file, line[])
                            d === nothing || push!(defs, d)
                        end
                    else
                        d = check_funsql_definition(b, file, line[])
                        d === nothing || push!(defs, d)
                    end
                end
            else
                d = check_funsql_definition(a, file, line[])
                d === nothing || push!(defs, d)
            end
        end
    elseif ex isa Expr && ex.head in (:function, :(=))
        d = check_julia_definition(ex, file, line[])
        d === nothing || push!(defs, d)
    elseif ex isa Expr && ex.head in (:block, :toplevel, :module)
        foreach(a -> collect_definitions!(defs, a, file, line), ex.args)
    end
    defs
end

"""
    funsql_definitions(text; file = "none")

Parse `text` and return a `FunSQLDefinition` for every definition that
builds queries with `@funsql`, each with the parameters it uses without
`\$` (`uses`, empty when there are none).
"""
funsql_definitions(text::AbstractString; file = "none") =
    collect_definitions!(
        FunSQLDefinition[], Meta.parseall(text, filename = file), file, Ref(0))

"""
    included_sources(path)

`path` and the files it `include`s, in order. A commented-out `include`
isn't followed.
"""
function included_sources(path)
    files = [path]
    function walk(ex)
        if ex isa Expr && ex.head == :call && ex.args[1] == :include && ex.args[2] isa String
            push!(files, joinpath(dirname(path), ex.args[2]))
        elseif ex isa Expr
            foreach(walk, ex.args)
        end
    end
    walk(Meta.parseall(read(path, String), filename = path))
    files
end
