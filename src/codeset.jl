struct CodeSets
    name::Symbol
    sets::Dict{Symbol, FunSQL.SQLNode}
    vals::Dict{Symbol, Vector{Concept}}
end

CodeSets(name) = CodeSets(name, Dict{Symbol, FunSQL.SQLNode}(), Dict{Symbol, Vector{Concept}}())

Base.push!(cs::CodeSets, p::Pair{Symbol, FunSQL.SQLNode}) = push!(cs.sets, p)
Base.push!(cs::CodeSets, p::Pair{Symbol, Vector{Concept}} = begin
    push!(cs.vals, p)
    push!(cs.sets, p[1] => FunSQL.From(p[2]))
end

function Base.convert(::Type{FunSQL.SQLNode}, cs::CodeSets)
    qs = FunSQL.SQLNode[]
    for q in values(cs.sets)
        push!(qs, @funsql($q.define_front(codeset => $cs.name)))
    end
    @funsql(append($qs...))
end

function concepts_cset_lookup(cset, args)
    ret = Concept[]
    if length(args) == 0
        return cset
    end
    for n in args
        if n isa FunSQL.SQLNode && getfield(n, :core) isa FunSQL.VariableNode
            n = getfield(n, :core).name
        end
        append!(ret, cset[n])
    end
    return ret
end

function build_concepts(df::DataFrame)
    retval = Concept[]
    sort!(df, [:vocabulary_id, :concept_code])
    for row in eachrow(df)
        vocabulary = Vocabulary(row.vocabulary_id)
        push!(retval, Concept(vocabulary, row.concept_id,
                              row.concept_code, row.concept_name,
                              !ismissing(row.standard_concept)))
    end
    return retval
end

function build_codesets(@nospecialize(expr), ctx)
    if @dissect(expr, Expr(:(=), Expr(:call, block_name::Symbol), Expr(:block, src, body...)))
        block_name = nothing
        @assert @dissect(expr, Expr(:block, src, body...))
    end
    ctx = FunSQL.TransliterateContext(ctx, src = src)
    priors = Dict{Symbol, Any}() #Vector{Concept}}()
    parts = Expr[]
    queries = Expr[]
    push!(parts, :(cs = CodeSets($block_name)))
    for expr in body
        expr isa LineNumberNode ? continue : nothing
        if @dissect(expr, Expr(:(=), cset_name::Symbol, query)) || # backward compatibility
           @dissect(expr, Expr(:(=), Expr(:call, cset_name::Symbol), Expr(:block, src, query)))
            #ctx = FunSQL.TransliterateContext(ctx, src = src)

            # backward compatibility with [] syntax
            if @dissect(query, Expr(:vect, items...))
                for (index, value) in enumerate(items)
                    if value isa Symbol
                        if value in keys(priors)
                            query.args[index] = Expr(:(...), priors[value])
                        else
                            error("bare $value must be mentioned previously")
                        end
                    elseif @dissect(value, Expr(:call, fname::Symbol))
                        if fname in keys(priors)
                            query.args[index] = Expr(:(...), priors[fname])
                        else
                            error("bare $fname() must be mentioned previously")
                        end
                    elseif @dissect(value, Expr(:call, fname::Symbol, args...))
                        query.args[index] = Expr(:call, Expr(:(.), @__MODULE__, 
                                                QuoteNode(:lookup_by_code)),
                                                QuoteNode(fname), args...)
                    else
                        error("expecting only concept functions")
                    end
                end
            else
                # forward compatibility when everything is a query
                query = FunSQL.transliterate(query, ctx)
            end
            fcall = Expr(:call, Expr(:escape, Symbol("funsql_$cset_name")))
            push!(queries, Expr(:(=), fcall, query))
            priors[cset_name] = query
            push!(parts, Expr(:call, esc(:(=>)), QuoteNode(cset_name), fcall))
        else
            error("expecting name() = funsql or name() = [concept...] assignments")
        end
    end
    value = Expr(:tuple, Expr(:parameters, parts...))
    block = Expr(:block, queries...)
    if isnothing(block_name)
        push!(block.args, value)
    else
       fname = Expr(:escape, Symbol("funsql_$block_name"))
       push!(block.args, :(cset = $value))
       push!(block.args, :($fname(args...) = TRDW.concepts_cset_lookup(cset, args)))
       push!(block.args, :(cset))
   end
   return block
end

macro concepts(expr::Expr)
    build_codesets(expr, FunSQL.TransliterateContext(__module__, __source__))
end

macro codesets(expr::Expr)
    build_codesets(expr, FunSQL.TransliterateContext(__module__, __source__))
end

function build_or(items)
    if length(items) == 0
        return @funsql(false)
    end
    if length(items) == 1
        return items[1]
    end
    return @funsql(or($items...))
end

function funsql_span(cs...; join=true, icdgem=true)
    buckets = Dict{Vocabulary, Vector{Concept}}()
    for c in unnest_concept_set(cs)
        push!(get!(buckets, c.vocabulary, Concept[]), c)
    end
    qs = FunSQL.SQLNode[]
    join = join ? @funsql(as(base).join(concept(), concept_id == base.concept_id)) : @funsql(define())
    for (v, cs) in pairs(buckets)
        ids = [c.concept_id for c in cs]
        push!(qs, @funsql begin
            from(concept_ancestor)
            filter(in(ancestor_concept_id, $ids...))
            select(concept_id => descendant_concept_id)
            $join
        end)
        if v.vocabulary_id in ("ICD9CM", "ICD10CM", "ICD9Proc", "ICD10PCS", "ICD03")
            cs = ["$(c.concept_code)%" for c in cs]
            tests = build_or([@funsql(like(concept_code, $m)) for m in cs])
            push!(qs, @funsql begin
                concept()
                filter(vocabulary_id == $(v.vocabulary_id))
                filter($tests)
            end)
            # TODO: ICD9Proc - ICD10PCS gem
            if v.vocabulary_id == "ICD10CM" && icdgem
                push!(qs, @funsql begin
                    $(qs[end])
                    join(cr => begin
                        from(concept_relationship)
                        filter(relationship_id == "ICD9CM - ICD10CM gem")
                    end, cr.concept_id_2 == concept_id)
                    select(concept_id => cr.concept_id_1)
                    $join
                end)
            end
        end
    end
    length(qs) == 0 ? @funsql(concept().filter(false)) :
    length(qs) == 1 ? qs[1] :
    @funsql(append($qs...).deduplicate(concept_id))
end

function concept_matches(match...; match_on=[], span=true)
    match = unnest_concept_set(match)
    if match_on isa FunSQL.SQLNode
        match_on = [match_on]
    elseif match_on isa Symbol
        if contains(string(match_on), "concept_id")
            match_on = [match_on]
        else
            match_on = [Symbol("$(match_on)_concept_id")]
        end
    else
        if isnothing(match_on) || length(match_on) == 0
            match_on = Any[@funsql(concept_id)]
            if any([contains(c.vocabulary.vocabulary_id, "ICD") for c in match])
                push!(match_on, @funsql(ext.icd_concept_id))
            end
        end
        @assert match_on isa Vector
    end
    match = span ? funsql_span(match) : match
    parts = [ @funsql(in($col, $match.select(concept_id))) for col in match_on]
    build_or(parts)
end

concept_matches(name::Symbol, match...) =
    concept_matches(match...; match_on=name)

const funsql_concept_matches = concept_matches

function funsql_concept_in(concept_id::Symbol, ids)
    if ids isa ConceptMatchExpr
        ids = unnest_concept_ids(ids)
    end
    @funsql(filter(in($concept_id, $ids...)))
end

funsql_concept_in(q) =
    funsql_concept_in(:concept_id, q)
