const VOCAB_SCHEMA = "vocabulary_v20230531"
const CONCEPT_PATH = (tempdir(), VOCAB_SCHEMA)
mkpath(joinpath(CONCEPT_PATH))

normalize_name(s) = replace(lowercase(s), r"[ -]" => "_")
cache_filename(s) = joinpath([CONCEPT_PATH..., "$(normalize_name(s)).csv"])

mutable struct Vocabulary
    vocabulary_id::String
    constructor::String
    dataframe::Union{DataFrame, Nothing}
end

const g_vocabularies = Dict{String, Vocabulary}()

function Vocabulary(vocabulary_id; constructor=nothing)
    if haskey(g_vocabularies, vocabulary_id)
        return g_vocabularies[vocabulary_id]
    end
    constructor = something(constructor, "Vocabulary($(repr(vocabulary_id)))")
    return g_vocabularies[vocabulary_id] = Vocabulary(vocabulary_id, constructor, nothing)
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
        conn = connect_to_databricks(; catalog = "ctsi")
        cursor = DBInterface.execute(conn, """
            SELECT concept_id, concept_code, concept_name,
                standard_concept, domain_id, concept_class_id
            FROM $(VOCAB_SCHEMA).concept
            WHERE vocabulary_id = '$(vocabulary_id)'
            """)
        concepts = cursor_to_dataframe(cursor)
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
end

Base.convert(::Type{FunSQL.SQLNode}, c::Concept) = convert(FunSQL.SQLNode, c.concept_id)

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

function lookup_by_code(vocabulary::Vocabulary, concept_code, check_name=nothing)
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
                   result[1, :concept_name])
    if isnothing(check_name)
        return concept
    end
    check_name = normalize_name(check_name)
    concept_name = normalize_name(concept.concept_name)
    if startswith(concept_name, check_name)
        return concept
    end
    if occursin("...", check_name)
        (start, finish) = split(check_name, "...")
        if startswith(concept_name, start) && endswith(concept_name, finish)
           return concept
        end
    end
    throw(ArgumentError("'$concept_code' failed name check in vocabulary $vocabulary_id"))
end

(vocabulary::Vocabulary)(concept_code, check_name = nothing) =
    lookup_by_code(vocabulary, concept_code, check_name)

function lookup_by_name(vocabulary::Vocabulary, concept_prefix)::Concept
    vocabulary_id = getfield(vocabulary, :vocabulary_id)
    vocabulary_data = vocabulary_data!(vocabulary)
    concept_prefix = normalize_name(string(concept_prefix))
    test = row -> startswith(normalize_name(row.concept_name), concept_prefix)
    result = filter(test, vocabulary_data)
    nrows = size(result)[1]
    if nrows > 1
        throw(ArgumentError("'$concept_prefix' matched $nrows in vocabulary $vocabulary_id"))
    end
    if nrows < 1
        throw(ArgumentError("'$concept_prefix' did not match in vocabulary $vocabulary_id"))
    end
    return Concept(vocabulary,
                   result[1, :concept_id],
                   result[1, :concept_code],
                   result[1, :concept_name])
end

Base.getproperty(vocabulary::Vocabulary, concept_prefix::Symbol) =
    lookup_by_name(vocabulary, concept_prefix)

macro make_vocabulary(name)
    lname = replace(name, " " => "_")
    funfn = Symbol("funsql#$lname")
    label = Symbol(lname)
    quote
        $(esc(label)) = Vocabulary($name; constructor=$lname)
        $(esc(funfn))(key, check=nothing) =
            lookup_by_code($label, concept_code, check_name)
        export $(esc(funfn))
    end
end

@make_vocabulary("Race")
@make_vocabulary("Provider")
@make_vocabulary("RxNorm")
@make_vocabulary("HemOnc")
@make_vocabulary("SNOMED")
@make_vocabulary("ICD10CM")
@make_vocabulary("ICD9CM")
@make_vocabulary("CPT4")
@make_vocabulary("LOINC")
@make_vocabulary("ATC")
@make_vocabulary("NDFRT")
@make_vocabulary("Condition Status")
@make_vocabulary("RxNorm Extension")

@funsql category_isa(type, args::Union{Tuple, AbstractVector}, concept_id = :concept_id) =
    in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id, $(lookup_by_name(type, args)...)))
        select(descendant_concept_id)
    end)

function print_concepts(df::DataFrame)
    first = true
    sort!(df, [:vocabulary_id, :concept_name])
    for row in eachrow(df)
        !first && println(",")
        print(replace(row.vocabulary_id, " " => "_"))
        print("($(row.concept_code),\"$(row.concept_name)\")")
        first = false
    end
    println()
end
