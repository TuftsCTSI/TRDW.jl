const VOCAB_SCHEMA = "vocabulary_v20230531"
const CONCEPT_PATH = (tempdir(), VOCAB_SCHEMA)
mkpath(joinpath(CONCEPT_PATH))

normalize_name(s) = replace(lowercase(s), r"[ -]" => "_")
cache_filename(s) = joinpath([CONCEPT_PATH..., "$(normalize_name(s)).csv"])

abstract type AbstractCategory end

mutable struct Vocabulary <: AbstractCategory
    vocabulary_id::String
    constructor::String
    dataframe::Union{DataFrame, Nothing}
end

const g_vocabularies = Dict{String, Vocabulary}()
g_vocab_conn::Union{Nothing, ODBC.Connection} = nothing

function get_connection()
    global g_vocab_conn
    if isnothing(g_vocab_conn)
        g_vocab_conn = connect_to_databricks(; catalog = "ctsi")
    end
    return g_vocab_conn
end

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
        conn = get_connection()
        cursor = DBInterface.execute(conn, """
            SELECT concept_id, concept_code, concept_name,
                standard_concept, domain_id, concept_class_id
            FROM $(VOCAB_SCHEMA).concept
            WHERE vocabulary_id = '$(vocabulary_id)'
            """)
        concepts = cursor_to_dataframe(cursor)
        CSV.write(vocabulary_filename, concepts)
        conn = cursor = nothing
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
                   result[1, :concept_name])
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
                   result[1, :concept_name])
end

function lookup_by_name(vocabulary::Vocabulary, match_name::String)::Concept
    concept = find_by_name(vocabulary, match_name)
    if isnothing(concept)
        vocabulary_id = getfield(vocabulary, :vocabulary_id)
        throw(ArgumentError("'$match_name' did not match in vocabulary $vocabulary_id"))
    end
    return concept
end

lookup_by_name(category::AbstractCategory, match_name::AbstractString) =
    lookup_by_name(category, String(match_name))
lookup_by_name(category::AbstractCategory, match_name::Symbol) =
    lookup_by_name(category, String(match_name))
lookup_by_name(category::AbstractCategory, concept::Concept) = concept
lookup_by_name(category::AbstractCategory, resolved::Int64) = resolved

function lookup_by_name(category::AbstractCategory, keys::AbstractVector)
    # TODO: de-duplicated flattened list... is there another way?
    retval = Vector{Union{Int64, Concept}}()
    for item in keys
        nest = lookup_by_name(category, item)
        if nest in retval
            continue
        end
        if nest isa AbstractVector
            for part in nest
                if part in retval
                    continue
                end
                push!(retval, part)
            end
            continue
        end
        push!(retval, nest)
    end
    return retval
end

macro make_vocabulary(name)
    lname = replace(name, " " => "_")
    funfn = Symbol("funsql#$lname")
    label = Symbol(lname)
    quote
        $(esc(label)) = Vocabulary($name; constructor=$lname)
        $(esc(funfn))(concept_code, match_name=nothing) =
            lookup_by_code($label, concept_code, match_name)
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

function build_concepts(df::DataFrame)
    retval = Concept[]
    sort!(df, [:vocabulary_id, :concept_name])
    for row in eachrow(df)
        vocabulary = Vocabulary(row.vocabulary_id)
        push!(retval, Concept(vocabulary, row.concept_id, row.concept_code, row.concept_name))
    end
    return retval
end

function build_concepts(conn, expr::Expr)
    block = Expr(:block)
    items = Expr(:tuple)
    if expr.head == :block
        exs = [ex for ex in expr.args if ex isa Expr]
    else
        exs = [expr]
    end
    for ex in exs;
        if @dissect(ex, Expr(:(=), name::Symbol, q))
            push!(items.args, Expr(:call, Symbol("=>"), QuoteNode(name), esc(name)))
            push!(block.args, :($(esc(name)) =
                        TRDW.build_concepts(TRDW.run($conn, @funsql $q))))
        else
            error("expecting name=funsql assignments")
        end
    end
    push!(block.args, items)
    return block
end
