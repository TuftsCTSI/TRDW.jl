const VOCAB_SCHEMA = "vocabulary_v20230823"
const CONCEPT_PATH = (tempdir(), VOCAB_SCHEMA)
mkpath(joinpath(CONCEPT_PATH))

normalize_name(s) = replace(lowercase(s), r"[ -]" => "_")
cache_filename(s) = joinpath([CONCEPT_PATH..., "$(normalize_name(s)).csv"])

abstract type AbstractCategory end

@enum MatchLocation begin
    MATCH_PRIMARY = 1
    MATCH_SOURCE = 2
    MATCH_BOTH = 3
end

mutable struct Vocabulary <: AbstractCategory
    vocabulary_id::String
    constructor::String
    dataframe::Union{DataFrame, Nothing}
    match_location::MatchLocation
    match_strategy::Function
end

const g_vocabularies = Dict{String, Vocabulary}()
const g_vocab_conn = Ref{FunSQL.SQLConnection}()

function vocab_connection()
    global g_vocab_conn
    if !isassigned(g_vocab_conn)
        g_vocab_conn[] = connect_with_funsql(VOCAB_SCHEMA; catalog = "ctsi")
    end
    return g_vocab_conn[]
end

match_descendants(concepts, concept_id::Symbol) =
    @funsql begin
        exists(begin
            from(concept_ancestor)
            filter(descendant_concept_id == :concept_id &&
                   in(ancestor_concept_id, $concepts...))
            bind(:concept_id => $concept_id)
        end)
    end

match_children(concepts, concept_id::Symbol) =
    @funsql begin
        exists(begin
            concept($concepts...)
            concept_children(0:5)
            filter(concept_id == :concept_id)
            bind(:concept_id => $concept_id)
        end)
    end

match_isa_relatives(concepts, concept_id::Symbol) =
    @funsql begin
        exists(begin
            from(concept_relationship)
            filter(relationship_id == "Is a" &&
                   concept_id_1 == :concept_id &&
                   in(concept_id_2, $concepts...))
            bind(:concept_id => $concept_id)
        end)
    end

function Vocabulary(vocabulary_id; constructor=nothing,
                    match_location=nothing, match_strategy=nothing)
    if haskey(g_vocabularies, vocabulary_id)
        return g_vocabularies[vocabulary_id]
    end
    constructor = something(constructor, "Vocabulary($(repr(vocabulary_id)))")
    return g_vocabularies[vocabulary_id] =
        Vocabulary(vocabulary_id, constructor, nothing,
                   something(match_location, MATCH_PRIMARY),
                   something(match_strategy, match_descendants))
end

function Base.show(io::IO, v::Vocabulary)
    print(io, "Vocabulary(")
    show(io, getfield(v, :vocabulary_id))
    print(io, ")")
end

function vocabulary_data!(vocabulary)
    vocabulary_data = getfield(vocabulary, :dataframe)
    if !isnothing(vocabulary_data)
        return vocabulary_data
    end
    vocabulary_id = getfield(vocabulary, :vocabulary_id)
    vocabulary_filename = cache_filename(vocabulary_id)
    if !isfile(vocabulary_filename)
        concepts = run(vocab_connection(),
                       @funsql(from(concept).filter(vocabulary_id==$vocabulary_id)))
        CSV.write(vocabulary_filename, concepts)
    end
    vocabulary_data = CSV.read(vocabulary_filename, DataFrame)
    setfield!(vocabulary, :dataframe, vocabulary_data)
    return vocabulary_data
end

struct Concept
    vocabulary::Vocabulary
    concept_id::Int64
    concept_code::Union{Int64, AbstractString}
    concept_name::AbstractString
    is_standard::Bool
end

unnest_concept_set(@nospecialize ids) = unnest_concept_set(ids, Vector{Concept}())
unnest_concept_set(c::Concept, cs::Vector{Concept}) = push!(cs, c)
unnest_concept_set(p::Pair, cs::Vector{Concept}) =
    unnest_concept_set(p[2], cs)
unnest_concept_set(items::Tuple, cs::Vector{Concept}) =
    unnest_concept_set(collect(items), cs)
unnest_concept_set(items::NamedTuple, cs::Vector{Concept}) =
    unnest_concept_set(collect(items), cs)
unnest_concept_set(items::Vector, cs::Vector{Concept}) =
    something(for it in items; unnest_concept_set(it, cs) end, cs)

Base.convert(::Type{FunSQL.SQLNode}, c::Concept) =
    convert(FunSQL.SQLNode, c.concept_name => c.concept_id)

function Base.show(io::IO, c::Concept)
    print(io, getfield(c.vocabulary, :constructor))
    print(io, "(")
    show(io, c.concept_code isa Integer ? c.concept_code : String(c.concept_code))
    print(io, ",")
    show(io, String(c.concept_name))
    print(io, ")")
end

function Base.repr(c::Concept)
    vname = "Vocabulary($(repr(c.vocabulary.vocabulary_id)))"
    return "$vname($(repr(c.concept_code)), $(repr(c.concept_name)))"
end

function is_concept_name_match(concept_name::AbstractString, match_name::String)
    concept_name = normalize_name(concept_name)
    if concept_name == match_name
        return true
    end
    if startswith(match_name, "...") && endswith(match_name, "...")
        match_name = match_name[4:end-3]
        if occursin(match_name, concept_name)
            return true
        end
        return false
    end
    if occursin("...", match_name)
        (start, finish) = split(match_name, "...")
        if startswith(concept_name, start) && endswith(concept_name, finish)
           return true
        end
    end
    return false
end

function lookup_by_code(vocabulary::Vocabulary, concept_code, match_name=nothing)
    vocabulary_id = getfield(vocabulary, :vocabulary_id)
    vocabulary_data = vocabulary_data!(vocabulary)
    if eltype(vocabulary_data.concept_code) <: Integer
        if !(concept_code isa Integer)
            concept_code = parse(Int, string(concept_code))
        end
        test = row -> row.concept_code == concept_code
    else
        concept_code = normalize_name(string(concept_code))
        test = row -> normalize_name(row.concept_code) == concept_code
    end
    result = filter(test, vocabulary_data)
    if 1 != size(result)[1]
        throw(ArgumentError("'$concept_code' not found in vocabulary $vocabulary_id"))
    end
    concept = Concept(vocabulary,
                   result[1, :concept_id],
                   result[1, :concept_code],
                   result[1, :concept_name],
                   !ismissing(result[1, :standard_concept]))
    if isnothing(match_name)
        return concept
    end
    if is_concept_name_match(concept.concept_name, normalize_name(match_name))
        return concept
    end
    throw(ArgumentError("'$concept_code' failed name check in vocabulary $vocabulary_id"))
end

(vocabulary::Vocabulary)(concept_code, match_name = nothing) =
    lookup_by_code(vocabulary, concept_code, match_name)

function find_by_name(vocabulary::Vocabulary, match_name::String;
            having::Union{Function, Nothing} = nothing)::Union{Concept, Nothing}
    vocabulary_id = getfield(vocabulary, :vocabulary_id)
    vocabulary_data = vocabulary_data!(vocabulary)
    match_name = normalize_name(match_name)
    if isnothing(having)
        test = row -> is_concept_name_match(row.concept_name, match_name)
    else
        test = row -> having(row) && is_concept_name_match(row.concept_name, match_name)
    end
    result = filter(test, vocabulary_data)
    nrows = size(result)[1]
    if nrows > 1
        throw(ArgumentError("'$match_name' matched $nrows in vocabulary $vocabulary_id"))
    end
    if nrows < 1
        return nothing
    end
    return Concept(vocabulary,
                   result[1, :concept_id],
                   result[1, :concept_code],
                   result[1, :concept_name],
                   !ismissing(result[1, :standard_concept]))
end

function lookup_by_name(vocabulary::Vocabulary, match_name::String)::Vector{Concept}
    concept = find_by_name(vocabulary, match_name)
    if isnothing(concept)
        vocabulary_id = getfield(vocabulary, :vocabulary_id)
        throw(ArgumentError("'$match_name' did not match in vocabulary $vocabulary_id"))
    end
    return [concept]
end

lookup_by_name(category::AbstractCategory, concept::Concept) = [concept]
lookup_by_name(category::AbstractCategory, match_name::AbstractString) =
    lookup_by_name(category, String(match_name))
lookup_by_name(category::AbstractCategory, match_name::Symbol) =
    lookup_by_name(category, String(match_name))
lookup_by_name(category::AbstractCategory, items::Tuple) =
    lookup_by_name(category, collect(items))

function lookup_by_name(category::AbstractCategory, keys::AbstractVector)
    retval = Vector{Concept}()
    for key in keys
        for value in lookup_by_name(category, key)
            if value in retval
                continue
            end
            push!(retval, value)
        end
    end
    return retval
end

macro make_vocabulary(name, match_location=nothing, match_strategy=nothing)
    lname = replace(name, " " => "_")
    funfn = Symbol("funsql#$lname")
    label = Symbol(lname)
    quote
        $(esc(label)) = Vocabulary($name; constructor=$lname,
                                   match_location=$match_location,
                                   match_strategy=$match_strategy)
        $(esc(funfn))(concept_code, match_name=nothing) =
            lookup_by_code($label, concept_code, match_name)
        export $(esc(funfn))
    end
end

@make_vocabulary("ATC")
@make_vocabulary("CPT4")
@make_vocabulary("Condition Status")
@make_vocabulary("HemOnc")
@make_vocabulary("ICD10CM", MATCH_SOURCE, match_isa_relatives)
@make_vocabulary("ICD10PCS")
@make_vocabulary("ICD9CM", MATCH_SOURCE, match_isa_relatives)
@make_vocabulary("LOINC")
@make_vocabulary("NDFRT")
@make_vocabulary("OMOP Extension")
@make_vocabulary("Provider")
@make_vocabulary("Race")
@make_vocabulary("RxNorm Extension")
@make_vocabulary("RxNorm")
@make_vocabulary("SNOMED")
@make_vocabulary("Type Concept")
@make_vocabulary("CMS Place of Service")
@make_vocabulary("Visit")

struct Category <: AbstractCategory
    name::String
    vocabs::Tuple{Vararg{Vocabulary}}
    having::Function
end

function Base.show(io::IO, c::Category)
    print(io, "Category(")
    show(io, getfield(v, :name))
    print(io, ")")
end

(category::Category)(keys...) = lookup_by_name(category, collect(keys))

function lookup_by_name(category::Category, match_name::String)::Concept
    name = getfield(category, :name)
    concept = nothing
    for vocab in getfield(category, :vocabs)
        match = find_by_name(vocab, String(match_name);
                             having = getfield(category, :having))
        if isnothing(match)
            continue
        end
        if isnothing(concept)
            concept = match
            continue
        end
        throw(ArgumentError("'$match_name' ambiguous in category $name"))
    end
    if isnothing(concept)
        throw(ArgumentError("'$match_name' failed to match in category $name"))
    end
    return concept
end

instr(x::Union{AbstractString, Missing}, values::String...) =
        ismissing(x) ? false : coalesce(x) in values

standard_domain(row, domain_id) =
    (row.domain_id == domain_id) && instr(row.standard_concept, "C", "S")

# useful concept classes, to increase readability
DoseFormGroup = Category("Dose Form Group", (RxNorm,),
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Dose Form Group")
ComponentClass = Category("ComponentClass", (HemOnc,),
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Component Class")
Ingredient = Category("Ingredient", (RxNorm, RxNorm_Extension),
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Ingredient")

var"funsql#Ingredient"(items...) = Ingredient(items...)
export var"funsql#Ingredient"

var"funsql#DoseFormGroup"(items...) = DoseFormGroup(items...)
export var"funsql#DoseFormGroup"

var"funsql#component_class"(items...) = ComponentClass(items...)
export var"funsql#ComponentClass"

@funsql category_isa(type, args::Union{Tuple, AbstractVector}, concept_id = :concept_id) =
    in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id, $(lookup_by_name(type, args)...)))
        select(descendant_concept_id)
    end)

function print_concepts(df::DataFrame, prefix="        ")
    first = true
    sort!(df, [:vocabulary_id, :concept_name])
    for row in eachrow(df)
        !first && println(",")
        print(prefix)
        print(replace(row.vocabulary_id, " " => "_"))
        print("($(row.concept_code),\"$(row.concept_name)\")")
        first = false
    end
    println()
end

print_concepts(q::FunSQL.SQLNode, prefix="        ") =
    print_concepts(run(vocab_connection(), q), prefix)

print_concepts(ids::Vector{<:Integer}, prefix="        ") =
    print_concepts(@funsql(concept($ids...)), prefix)

function build_concepts(df::DataFrame)
    retval = Concept[]
    sort!(df, [:vocabulary_id, :concept_name])
    for row in eachrow(df)
        vocabulary = Vocabulary(row.vocabulary_id)
        push!(retval, Concept(vocabulary, row.concept_id,
                              row.concept_code, row.concept_name,
                              !ismissing(row.standard_concept)))
    end
    return retval
end

macro concepts(expr::Expr)
    block = Expr(:tuple)
    conn = vocab_connection()
    if expr.head == :block
        exs = [ex for ex in expr.args if ex isa Expr]
    else
        exs = [expr]
    end
    for ex in exs;
        if @dissect(ex, Expr(:(=), name::Symbol, query))
            if @dissect(query, Expr(:vect, args...))
                for (index, value) in enumerate(query.args)
                    if @dissect(value, Expr(:(...), item, _...)) && item isa Symbol
                        error("no need to ... expand references to arrays within @concepts")
                    end
                    if value isa Symbol
                        query.args[index] = Expr(:(...), esc(value))
                    end
                end
                item = query
            elseif @dissect(query, Expr(:tuple, args...))
                query.head = :vect
                item = query
            else
                item = :(build_concepts(run($conn, @funsql $query)))
            end
            push!(block.args, Expr(:(=), name, item))
        else
            error("expecting name=funsql or name=[concept...] assignments")
        end
    end
    return block
end

#Base.convert(::Type{FunSQL.SQLNode}, v::NamedTuple{<:Any, <:Tuple{Vararg{Vector{Concept}}}}) =
#   FunSQL.Lit(join(string.([c.concept_id for c in Iterators.flatten(values(v))]), ", "))

function concept_matches(match...; match_prefix=nothing, match_source=nothing)
    match = unnest_concept_set(match)
    match_prefix = isnothing(match_prefix) ? :concept_id : match_prefix
    if contains(string(match_prefix), "concept_id")
        concept_id = Symbol(match_prefix)
        if isnothing(match_source)
            source_concept_id = concept_id
        else
            @assert contains(string(match_prefix), "concept_id")
            source_concept_id = Symbol(match_source)
        end
    else
        @assert isnothing(match_source)
        concept_id = Symbol("$(match_prefix)_concept_id")
        source_concept_id = Symbol("$(match_prefix)_source_concept_id")
    end
    buckets = Dict()
    non_standard = Dict()
    for c in match
        key = (getfield(c.vocabulary, :match_strategy),
               (concept_id == source_concept_id) ?
                   MATCH_PRIMARY :
                   getfield(c.vocabulary, :match_location))
        if !c.is_standard && key[1] == match_descendants
            key = (match_children, key[2])
        end
        bucket = haskey(buckets, key) ?
                   buckets[key] :
                   (buckets[key] = Int[])
        push!(bucket, c.concept_id)
    end
    tests = FunSQL.SQLNode[]
    for ((strategy, location), ids) in buckets
        if location in (MATCH_PRIMARY, MATCH_BOTH)
            push!(tests, strategy(ids, concept_id))
        end
        if location in (MATCH_SOURCE, MATCH_BOTH)
            push!(tests, strategy(ids, source_concept_id))
        end
    end
    if length(tests) == 0
        return @funsql(false)
    end
    if length(tests) == 1
        return tests[1]
    end
    return FunSQL.Fun.Or(tests...)
end
const var"funsql#concept_matches" = concept_matches

