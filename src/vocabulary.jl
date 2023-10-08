const VOCAB_SCHEMA = "vocabulary_v20230531"
CONCEPT_PATH = [tempdir(), ENV["USERNAME"], "$(VOCAB_SCHEMA)"]
g_concepts = nothing

cache_filename(s) = joinpath([TRDW.CONCEPT_PATH..., "$(lowercase(string(s))).csv"])

""" load entire concept table, caching locally to CSV file"""
function load_concepts()
    global g_concepts
    if g_concepts != nothing
        return g_concepts
    end
    if !isfile(cache_filename("concept"))
        conn = connect_to_databricks(; catalog = "ctsi")
        cursor = DBInterface.execute(conn, "SELECT * FROM $(VOCAB_SCHEMA).concept")
        concepts = cursor_to_dataframe(cursor)
        mkpath(joinpath(CONCEPT_PATH))
        CSV.write(cache_filename("concept"), concepts)
        concepts = nothing
    end
    g_concepts = CSV.read(cache_filename("concept"), DataFrame)
    return g_concepts
end

normalize_concept_name(s::AbstractString) = replace(lowercase(s), r"[ -]" => "_")

struct Vocabulary
    name::String
    table::Dict{Symbol, Tuple{Int64, String}}
    
    function Vocabulary(name::String)
        new(name, Dict{Symbol, Tuple{Int64, String}}())
    end
end

function read_concepts(name, rebuild, process)
    filename = cache_filename(name)
    if !isfile(filename) || rebuild
        concepts = process(load_concepts())
        mkpath(joinpath(CONCEPT_PATH))
        CSV.write(filename, concepts)
    end
    CSV.read(filename, DataFrame)
end

function load_vocabulary(vocabulary::Vocabulary; rebuild=false)
    name = getfield(vocabulary, :name)
    table = getfield(vocabulary, :table)
    if isempty(table) || rebuild
        empty!(table)
        concepts = read_concepts(name, rebuild,
            concepts -> select(filter(row -> row.vocabulary_id == name, concepts),
                               [:concept_id, :concept_code, :concept_name]))
        for row in eachrow(concepts)
            key = Symbol(row.concept_code)
            haskey(table, key) && @error "\nDuplicate $name:\n$key"
            table[key] = (row.concept_id, normalize_concept_name(row.concept_name))
        end
    end
end

function lookup(vocabulary::Vocabulary, key, check = nothing)::Int64
    load_vocabulary(vocabulary)
    name = getfield(vocabulary, :name)
    table = getfield(vocabulary, :table)
    key = Symbol(key)
    !haskey(table, key) && throw(ArgumentError("'$key' not found in $name"))
    (concept_id, concept_name) = table[key]
    check == nothing && return concept_id
    check = string(check)
    if occursin("...", check)
        (start, finish) = split(check, "...")
        if startswith(concept_name, normalize_concept_name(start)) &&
            endswith(concept_name, normalize_concept_name(finish))
            return concept_id
        end
    else
        startswith(concept_name, normalize_concept_name(check)) && return concept_id
    end
    throw(ArgumentError("'$key' in $name doesn't match '$check'"))
end

lookup(vocabulary::Vocabulary, key, check::Cmd) =
    lookup(vocabulary, key, join(check.exec, " "))

(vocabulary::Vocabulary)(key, check = nothing) = lookup(vocabulary, key, check)

struct Category
    name::String
    table::Dict{Symbol, Int64}
    filter::Function
    
    Category(name::String, filter::Function) = 
        new(name, Dict{Symbol, Int64}(), filter)
end

function load_category(category::Category; rebuild=false)
    name = getfield(category, :name)
    table = getfield(category, :table)
    if isempty(table) || rebuild
        empty!(table)
        concepts = read_concepts(name, rebuild, 
            concepts -> select(filter(getfield(category, :filter), concepts),
                               [:concept_id, :concept_name]))
        duplicates = Set{Symbol}()
        for row in eachrow(concepts)
            key = row.concept_name
            nothing == match(r"^[A-Za-z][\w\- ]*\w$", key) && continue
            key = Symbol(normalize_concept_name(key))
            key in duplicates && continue
            if haskey(table, key)
                push!(duplicates, key)
                delete!(table, key)
                continue
            end
            table[key] = row.concept_id
        end
    end
end

function Base.fieldnames(category::Category)
    load_category(category)
    return keys(getfield(category, :table))
end

function lookup(category::Category, key::Symbol)::Int64
    load_category(category)
    name = getfield(category, :name)
    table = getfield(category, :table)
    !haskey(table, key) && throw(ArgumentError("'$key' not found in $name"))
    return table[key]
end

Base.getproperty(category::Category, key::Symbol) = lookup(category, name)

function lookup(category::Category, key::AbstractString)::Int64
    lookup(category, Symbol(normalize_concept_name(key)))
end

function lookup(category::Category, keys::AbstractVector)
    [lookup(category, key) for key in keys]
end

function lookup(category::Category, keys::Tuple)
    [lookup(category, key) for key in keys]
end

instr(x::Union{AbstractString, Missing}, values::String...) =
        ismissing(x) ? false : coalesce(x) in values

standard_domain(row, domain_id) = 
    (row.domain_id == domain_id) && instr(row.standard_concept, "C", "S")

Race = Category("Race", 
    row -> standard_domain(row, "Race") && row.concept_id < 10000)
Ethnicity = Category("Ethnicity", 
    row -> standard_domain(row, "Ethnicity"))
ConditionStatus = Category("ConditionStatus",
    row -> standard_domain(row, "Condition Status"))
Specialty = Category("Specialty", 
    row -> standard_domain(row, "Provider"))
DoseFormGroup = Category("DoseFormGroup",
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Dose Form Group")
ComponentClass = Category("ComponentClass", 
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Component Class")
Ingredient = Category("Ingredient", 
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Ingredient" &&
           row.vocabulary_id == "RxNorm")

ingredient(args...) = lookup(Ingredient, args)
component_class(args...) = lookup(ComponentClass, args)
condition_status(args...) = lookup(ConditionStatus, args)
dose_form_group(args...) = lookup(DoseFormGroup, args)
race(args...) = lookup(Race, args)
specialty(args...) = lookup(Specialty, args)
ethnicity(args...) = lookup(Ethnicity, args)

RxNorm = Vocabulary("RxNorm")
HemOnc = Vocabulary("HemOnc")
SNOMED = Vocabulary("SNOMED")
ICD10CM = Vocabulary("ICD10CM")
ICD9CM = Vocabulary("ICD9CM")
CPT4 = Vocabulary("CPT4")
LOINC = Vocabulary("LOINC")
ATC = Vocabulary("ATC")
RxNormExtension = Vocabulary("RxNorm Extension")

@funsql RxNorm(code, check=nothing) = $(something(check, code) => TRDW.RxNorm(code, check))
@funsql HemOnc(code, check=nothing) = $(something(check, code) => TRDW.HemOnc(code, check))
@funsql SNOMED(code, check=nothing) = $(something(check, code) => TRDW.SNOMED(code, check))
@funsql ICD10CM(code, check=nothing) = $(something(check, code) => TRDW.ICD10CM(code, check))
@funsql ICD9CM(code, check=nothing) = $(something(check, code) => TRDW.ICD9CM(code, check))
@funsql CPT4(code, check=nothing) = $(something(check, code) => TRDW.CPT4(code, check))
@funsql LOINC(code, check=nothing) = $(something(check, code) => TRDW.LOINC(code, check))
@funsql ATC(code, check=nothing) = $(something(check, code) => TRDW.ATC(code, check))
@funsql RxNormExtension(code, check=nothing) = 
    $(something(check, code) => TRDW.RxNormExtension(code, check))

@funsql in_category(type, args, concept_id = :concept_id) =
    in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id, $(lookup(type, args)...)))
        select(descendant_concept_id)
    end)

function print_concepts(df::DataFrame)
    first = true
    for row in eachrow(df)
        !first && println(",")
        print(replace(row.vocabulary_id, " " => "")) 
        print("($(row.concept_code),\"$(row.concept_name)\")")             
        first = false
    end
    println()
end
