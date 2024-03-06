const VOCAB_SCHEMA = "vocabulary_v20230823"
const CONCEPT_PATH = (tempdir(), VOCAB_SCHEMA)

normalize_name(s) = replace(lowercase(s), r"[ -]" => "_")
cache_filename(s) = joinpath([CONCEPT_PATH..., "$(normalize_name(s)).csv"])

abstract type AbstractConcept end
abstract type AbstractCategory end

mutable struct Vocabulary <: AbstractCategory
    constructor::Symbol
    vocabulary_id::String
    concept_cache::Dict{String, T} where {T <: AbstractConcept}
    dataframe::Union{DataFrame, Nothing}
end

struct Concept <: AbstractConcept
    vocabulary::Vocabulary
    concept_id::Int64
    concept_code::AbstractString
    concept_name::AbstractString
    is_standard::Bool
end

Base.isless(lhs::Vocabulary, rhs::Vocabulary) =
    isless(lhs.vocabulary_id, rhs.vocabulary_id)

const g_vocabularies = Dict{Symbol, Vocabulary}()
const g_vocab_conn = Ref{FunSQL.SQLConnection}()

function vocab_connection()
    global g_vocab_conn
    if !isassigned(g_vocab_conn)
        g_vocab_conn[] = connect_with_funsql(VOCAB_SCHEMA; catalog = "ctsi")
    end
    return g_vocab_conn[]
end

function Vocabulary(vocabulary_id::String; constructor=nothing)
    key = Symbol(replace(vocabulary_id, " " => "_"))
    if haskey(g_vocabularies, key)
        return g_vocabularies[key]
    end
    return g_vocabularies[key] = Vocabulary(key, vocabulary_id, Dict{String, Concept}(), nothing)
end

function Vocabulary(key::Symbol)
    if haskey(g_vocabularies, key)
        return g_vocabularies[key]
    end
    vocabulary_id = :GCN_SEQNO ? "GCN_SEQNO" : replace(string(key), "_" => " ")
    return g_vocabularies[key] = Vocabulary(key, vocabulary_id, Dict{String, Concept}(), nothing)
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
        mkpath(joinpath(CONCEPT_PATH))
        concepts = run(vocab_connection(),
                       @funsql(from(concept).filter(vocabulary_id==$vocabulary_id)))
        CSV.write(vocabulary_filename, concepts)
    end
    column_types = Dict("concept_code" => String)
    vocabulary_data = CSV.read(vocabulary_filename, DataFrame; types=column_types)
    setfield!(vocabulary, :dataframe, vocabulary_data)
    return vocabulary_data
end

const ConceptSet = Vector{Concept}

Base.isless(lhs::Concept, rhs::Concept) =
    lhs.vocabulary != rhs.vocabulary ? isless(lhs.vocabulary, rhs.vocabulary) :
    isless(lowercase(lhs.concept_name), lowercase(rhs.concept_name))

Base.getproperty(c::Concept, name::Symbol) =
    name == :vocabulary_id ? c.vocabulary.vocabulary_id : getfield(c, name)

Tables.istable(::Type{ConceptSet}) = true
Tables.rowaccess(::Type{ConceptSet}) = true
Tables.istable(::Type{Concept}) = true
Tables.rowaccess(::Type{Concept}) = true
Tables.rows(c::Concept) = [c]
Tables.columnnames(::Concept) =
    (:concept_id, :vocabulary_id, :concept_code, :concept_name)
Tables.columnnames(::ConceptSet) =
    (:concept_id, :vocabulary_id, :concept_code, :concept_name)
Tables.getcolumn(c::Concept, i::Int) = Tables.getcolumn(c, columnnames(c)[i])

Base.convert(::Type{FunSQL.SQLNode}, c::Concept) = @funsql(from($c))
Base.convert(::Type{FunSQL.SQLNode}, vc::Vector{Concept}) = @funsql(from($vc))

DBInterface.execute(conn::FunSQL.SQLConnection{T}, c::Concept) where {T} =
    DBInterface.execute(conn, [c])
DBInterface.execute(conn::FunSQL.SQLConnection{T}, vc::ConceptSet) where {T} =
    DBInterface.execute(conn, @funsql(from($vc)))

function Base.show(io::IO, c::Concept)
    print(io, getfield(c.vocabulary, :constructor))
    print(io, "(")
    show(io, c.concept_code isa Integer ? c.concept_code : String(c.concept_code))
    print(io, ",")
    show(io, String(c.concept_name))
    print(io, ")")
end

const NamedConceptSets = NamedTuple{T, <:NTuple{N, ConceptSet}} where {N, T}
const ConceptMatchExpr = Union{Concept, ConceptSet, NamedConceptSets}

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

function unnest_concept_set(node::FunSQL.SQLNode, cs::Vector{Concept})
    conn = vocab_connection()
    cset = build_concepts(run(conn, node))
    append!(cs, cset)
end

@nospecialize
function Base.show(io::IO, m::MIME"text/html", ncs::T) where T <: NamedConceptSets
    print(io, """
    <div style="overflow:scroll;max-height:500px;">
    <table><tr><th><i>variable</i></th>
    <th>concept_id</th>
    <th>vocabulary_id</th>
    <th>concept_code</th>
    <th>concept_name</th></tr>
    """)
    for k in keys(ncs)
        vc = getfield(ncs, k)
        sort!(vc)
        for n in 1:length(vc)
            c = vc[n]
            show(io, m,
                @htl("""
                  <tr>
                      $(n==1 ? @htl("""
                              <td rowspan=$(length(vc))
                                  style=$(length(vc)>1 ?
                                          @htl("vertical-align: top") : "")>
                              <i>$k</i></td>
                          """) : "")
                      <td>$(c.concept_id)</td>
                      <td>$(c.vocabulary.vocabulary_id)</td>
                      <td>$(c.concept_code)</td>
                      <td>$(c.concept_name)</td>
                  </tr>
                """))
        end
    end
    print(io, "</table></div>")
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

function lookup_vsac_code(vocabulary::Vocabulary, concept_code)
    vocabulary_data = vocabulary_data!(vocabulary)
    concept_code = normalize_name(string(concept_code))
    test = row -> normalize_name(row.concept_code) == concept_code
    result = filter(test, vocabulary_data)
    if 1 != size(result)[1]
        return nothing
    end
    Concept(vocabulary,
            result[1, :concept_id],
            result[1, :concept_code],
            result[1, :concept_name],
            !ismissing(result[1, :standard_concept]))
end

function lookup_by_code(vocabulary::Vocabulary, concept_code, match_name=nothing)
    vocabulary_id = getfield(vocabulary, :vocabulary_id)
    vocabulary_data = vocabulary_data!(vocabulary)
    if concept_code == nothing
        match_name = normalize_name(match_name)
        test = row -> is_concept_name_match(row.concept_name, match_name)
        result = filter(test, vocabulary_data)
        if 1 != size(result)[1]
            throw(ArgumentError("'$match_name' not singular in vocabulary $vocabulary_id"))
        end
    else
        concept_code = normalize_name(string(concept_code))
        #if haskey(vocabulary.concept_cache, concept_code)
        #    return vocabulary.concept_cache[concept_code]
        #end
        test = row -> normalize_name(row.concept_code) == concept_code
        result = filter(test, vocabulary_data)
        if 1 != size(result)[1]
            throw(ArgumentError("'$concept_code' not found in vocabulary $vocabulary_id"))
        end
    end
    concept = Concept(vocabulary,
                   result[1, :concept_id],
                   result[1, :concept_code],
                   result[1, :concept_name],
                   !ismissing(result[1, :standard_concept]))
    vocabulary.concept_cache[concept_code] = concept
    if isnothing(match_name)
        return concept
    end
    if is_concept_name_match(concept.concept_name, normalize_name(match_name))
        return concept
    end
    throw(ArgumentError("'$concept_code' failed name check in vocabulary $vocabulary_id"))
end

lookup_by_code(vocab, concept_code, match_name=nothing) =
    lookup_by_code(Vocabulary(vocab), concept_code, match_name)

(vocab::Vocabulary)(concept_code, match_name=nothing) =
    lookup_by_code(vocab, concept_code, match_name)

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

function lookup_by_name(vocabulary::Vocabulary, match_name::String)
    concept = find_by_name(vocabulary, match_name)
    if isnothing(concept)
        vocabulary_id = getfield(vocabulary, :vocabulary_id)
        throw(ArgumentError("'$match_name' did not match in vocabulary $vocabulary_id"))
    end
    return [concept]
end

lookup_by_name(category::AbstractCategory, concept::Concept)::Vector{Concept} = [concept]
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

macro make_vocabulary(name)
    lname = replace(name, " " => "_")
    funfn = Symbol("funsql_$lname")
    label = Symbol(lname)
    quote
        $(esc(label)) = Vocabulary($name; constructor=$lname)
        $(esc(funfn))(concept_code, match_name=nothing) =
            lookup_by_code($label, concept_code, match_name)
        export $(esc(funfn))
    end
end

@make_vocabulary("ABMS")
@make_vocabulary("ATC")
@make_vocabulary("CMS Place of Service")
@make_vocabulary("CPT4")
@make_vocabulary("Condition Status")
@make_vocabulary("HES Specialty")
@make_vocabulary("HemOnc")
@make_vocabulary("ICD03")
@make_vocabulary("ICD10CM")
@make_vocabulary("ICD10PCS")
@make_vocabulary("ICD9CM")
@make_vocabulary("ICD9Proc")
@make_vocabulary("LOINC")
@make_vocabulary("Medicare Specialty")
@make_vocabulary("NDFRT")
@make_vocabulary("NUCC")
@make_vocabulary("None")
@make_vocabulary("OMOP Extension")
@make_vocabulary("Procedure Type")
@make_vocabulary("Provider")
@make_vocabulary("Race")
@make_vocabulary("RxNorm Extension")
@make_vocabulary("RxNorm")
@make_vocabulary("SNOMED")
@make_vocabulary("Type Concept")
@make_vocabulary("UCUM")
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

function lookup_by_name(category::Category, match_name::String)
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
    return [concept]
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
Route = Category("Route", (SNOMED,),
    row -> standard_domain(row, "Route") &&
           row.concept_class_id == "Qualifier Value")
Specialty = Category("Specialty", (Provider, NUCC, HES_Specialty, Medicare_Specialty, ABMS),
    row -> standard_domain(row, "Provider"))

funsql_Ingredient(items...) = Ingredient(items...)
export funsql_Ingredient
funsql_DoseFormGroup(items...) = DoseFormGroup(items...)
export funsql_DoseFormGroup
funsql_component_class(items...) = ComponentClass(items...)
export funsql_ComponentClass
funsql_Route(items...) = Route(items...)
export funsql_Route
funsql_Specialty(items...) = Specialty(items...)
export funsql_Specialty

function funsql_category_isa(type, cs::Union{Tuple, AbstractVector}, concept_id = :concept_id)
    cs = [c.concept_id for c in TRDW.lookup_by_name(type, cs)]
    @funsql in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id, $cs...))
        select(descendant_concept_id)
    end)
end

function print_concepts(df::DataFrame, prefix="        ")
    first = true
    sort!(df, [:vocabulary_id, :concept_name])
    for row in eachrow(df)
        !first && println(",")
        print(prefix)
        print(replace(row.vocabulary_id, " " => "_"))
        print("(\"$(row.concept_code)\",\"$(row.concept_name)\")")
        first = false
    end
    println()
end

print_concepts(::Nothing) = nothing

print_concepts(q::FunSQL.SQLNode, prefix="        ") =
    print_concepts(run(vocab_connection(), q), prefix)

print_concepts(ids::Vector{<:Integer}, prefix="        ") =
    print_concepts(@funsql(concept($ids...)), prefix)

