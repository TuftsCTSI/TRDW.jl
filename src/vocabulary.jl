const VOCAB_SCHEMA = "vocabulary_v20230531"
CONCEPT_PATH = [tempdir(), VOCAB_SCHEMA]

normalize_name(s) = replace(lowercase(s), r"[ -]" => "_")
cache_filename(s) = joinpath([TRDW.CONCEPT_PATH..., "$(normalize_name(s)).csv"])

function load_vocabulary(vocabulary)
    if !isfile(cache_filename(vocabulary))
        conn = connect_to_databricks(; catalog = "ctsi")
        cursor = DBInterface.execute(conn, """
            SELECT concept_id, concept_code, concept_name,
                standard_concept, domain_id, concept_class_id
            FROM $(VOCAB_SCHEMA).concept
            WHERE vocabulary_id = '$(vocabulary)'
            """)
        concepts = cursor_to_dataframe(cursor)
        mkpath(joinpath(CONCEPT_PATH))
        CSV.write(cache_filename(vocabulary), concepts)
    end
    return CSV.read(cache_filename(vocabulary), DataFrame)
end

struct Category
    name::Symbol
    criteria::Union{Function, Nothing}
    vocabs::Vector{String}
    byname::Dict{Symbol, Int64}
    bycode::Dict{Symbol, Tuple{Int64, String}}

    function Category(name::String, vocabs = nothing, criteria = nothing)
        vocabs = something(vocabs, [name])
        name = Symbol(replace(name, " " => "_"))
        new(name, criteria, vocabs, Dict{Symbol, Int64}(),
            Dict{Symbol, Tuple{Int64, String}}())
    end
end

function load_category!(category::Category; rebuild=false)
    name = getfield(category, :name)
    bycode = getfield(category, :bycode)
    byname = getfield(category, :byname)
    criteria = getfield(category, :criteria)
    if isempty(bycode) || rebuild
        empty!(bycode)
        empty!(byname)
        concepts = DataFrame()
        for v in getfield(category, :vocabs)
            append!(concepts, load_vocabulary(v); promote=true)
        end
        if !isnothing(criteria)
            concepts = filter(criteria, concepts)
        end
        for row in eachrow(concepts)
            key = Symbol(row.concept_code)
            haskey(bycode, key) && @error "\nduplicate concept_code $name:\n$key"
            bycode[key] = (row.concept_id, normalize_name(row.concept_name))
        end
        duplicates = Set{Symbol}()
        for row in eachrow(concepts)
            key = row.concept_name
            nothing == match(r"^[A-Za-z][\w\- ]*\w$", key) && continue
            length(key) < 32 || continue
            key = Symbol(normalize_name(key))
            key in duplicates && continue
            if haskey(byname, key)
                push!(duplicates, key)
                delete!(byname, key)
                continue
            end
            byname[key] = row.concept_id
        end
    end
end

function lookup_by_code(category::Category, key, check = nothing)::Int64
    load_category!(category)
    name = getfield(category, :name)
    bycode = getfield(category, :bycode)
    key = Symbol(key)
    !haskey(bycode, key) && throw(ArgumentError("'$key' not found in $name"))
    (concept_id, concept_name) = bycode[key]
    check == nothing && return concept_id
    check = string(check)
    if occursin("...", check)
        (start, finish) = split(check, "...")
        if startswith(concept_name, normalize_name(start)) &&
            endswith(concept_name, normalize_name(finish))
            return concept_id
        end
    else
        startswith(concept_name, normalize_name(check)) && return concept_id
    end
    throw(ArgumentError("'$key' in $name doesn't match '$check'"))
end

(category::Category)(key, check = nothing) = lookup_by_code(category, key, check)

function Base.fieldnames(category::Category)
    load_category!(category)
    return keys(getfield(category, :byname))
end

function lookup_by_name(category::Category, key::Symbol)::Int64
    load_category!(category)
    name = getfield(category, :name)
    byname = getfield(category, :byname)
    !haskey(byname, key) && throw(ArgumentError("'$key' not found in $name"))
    return byname[key]
end

Base.getproperty(category::Category, key::Symbol) = lookup_by_name(category, key)

function lookup_by_name(category::Category, key::AbstractString)::Int64
    lookup_by_name(category, Symbol(normalize_name(key)))
end

function lookup_by_name(category::Category, keys::AbstractVector)::Vector{Int64}
    result = []
    for key in keys
        if isa(key, AbstractString) || isa(key, Symbol)
            append!(result, lookup_by_name(category, key))
        else
            append!(result, key)
        end
    end
    return result
end

lookup_by_name(category::Category, keys::Tuple) =
   lookup_by_name(category, collect(keys))

instr(x::Union{AbstractString, Missing}, values::String...) =
        ismissing(x) ? false : coalesce(x) in values

standard_domain(row, domain_id) =
    (row.domain_id == domain_id) && instr(row.standard_concept, "C", "S")

# useful concept classes, to increase readability
DoseFormGroup = Category("Dose Form Group", ["RxNorm"],
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Dose Form Group")
ComponentClass = Category("ComponentClass", ["HemOnc"],
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Component Class")
Ingredient = Category("Ingredient", ["RxNorm", "RxNorm Extension"],
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Ingredient")
var"funsql#ingredient"(names::Union{String, Symbol}...) =
    [Symbol(name) => TRDW.lookup_by_name(TRDW.Ingredient, name) for name in names]
export var"funsql#ingredient"

macro make_vocabulary(name)
    label = replace(name, " " => "_")
    funfn = Symbol("funsql#$label")
    label = Symbol(label)
    quote
        $(esc(label)) = Category($name)
        $(esc(funfn))(key, check=nothing) =
           (Symbol(something(check, key)) => lookup_by_code($label, key, check))
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

#=
Race = Category("Race")
Provider = Category("Provider")
Ethnicity = Category("Ethnicity")
RxNorm = Category("RxNorm")
HemOnc = Category("HemOnc")
SNOMED = Category("SNOMED")
ICD10CM = Category("ICD10CM")
ICD9CM = Category("ICD9CM")
CPT4 = Category("CPT4")
LOINC = Category("LOINC")
ATC = Category("ATC")
ConditionStatus = Category("Condition Status")
RxNormExtension = Category("RxNorm Extension")

@funsql Provider(code, check=nothing) = $(something(check, code) => TRDW.Provider(code, check))
@funsql Ethnicity(code, check=nothing) = $(something(check, code) => TRDW.Ethnicity(code, check))
@funsql RxNorm(code, check=nothing) = $(something(check, code) => TRDW.RxNorm(code, check))
@funsql HemOnc(code, check=nothing) = $(something(check, code) => TRDW.HemOnc(code, check))
@funsql SNOMED(code, check=nothing) = $(something(check, code) => TRDW.SNOMED(code, check))
@funsql ICD10CM(code, check=nothing) = $(something(check, code) => TRDW.ICD10CM(code, check))
@funsql ICD9CM(code, check=nothing) = $(something(check, code) => TRDW.ICD9CM(code, check))
@funsql CPT4(code, check=nothing) = $(something(check, code) => TRDW.CPT4(code, check))
@funsql LOINC(code, check=nothing) = $(something(check, code) => TRDW.LOINC(code, check))
@funsql ATC(code, check=nothing) = $(something(check, code) => TRDW.ATC(code, check))
@funsql ConditionStatus(code, check=nothing) =
    $(something(check, code) => TRDW.ConditionStatus(code, check))
@funsql RxNormExtension(code, check=nothing) =
    $(something(check, code) => TRDW.RxNormExtension(code, check))
=#

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
