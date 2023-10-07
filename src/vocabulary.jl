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

""" make a concept name into a valid lower-case variable name"""
function normalize_concept_name(s::AbstractString)::Symbol
    s = replace(lowercase(s), r"[ -]" => "_")
    s = (s -> occursin(r"^\d", s) ? "_" * s : s)(s)
    s = replace(s, r"_+" => "_")
    s = replace(s, r"_$" => "")
    return Symbol(s)
end

""" mapping of normalized concept_name to the concept_id """
struct ConceptLookup
    name::Symbol
    filter::Function
    table::Dict{Symbol, Int64}

    ConceptLookup(name::Symbol, filter::Function) =
        new(name, filter, Dict{Symbol, Integer}())
end

function build_table(bucket::ConceptLookup; rebuild=false)
    bname = getfield(bucket, :name)
    table = getfield(bucket, :table)
    if isempty(table) || rebuild
        filename = cache_filename(bname)
        if !isfile(filename) || rebuild
            concepts = load_concepts()
            concepts = filter(getfield(bucket, :filter), concepts)
            concepts = select(concepts, [:concept_name, :concept_id])
            mkpath(joinpath(CONCEPT_PATH))
            CSV.write(filename, concepts)
            concepts = nothing
        end
        empty!(table)
        concepts = CSV.read(filename, DataFrame)
        duplicates = Dict{Symbol, Vector{Int64}}()
        for row in eachrow(concepts)
            key = normalize_concept_name(row.concept_name)
            if haskey(duplicates, key)
                push!(duplicates[key], row.concept_id)
            elseif haskey(table, key)
                duplicates[key] = [table[key]]
                push!(duplicates[key], row.concept_id)
                delete!(table, key)
            else
                table[key] = row.concept_id
            end
        end
        if !isempty(duplicates)
            dups = ["$id : $values" for (id, values) in duplicates]
            dups = join(dups, "\n")
            @warn("\nDuplicates in $bname:\n$dups")
        end
    end
end

function Base.fieldnames(bucket::ConceptLookup)
    build_table(bucket)
    return keys(getfield(bucket, :table))
end

function lookup(bucket::ConceptLookup, name::Symbol)::Int64
    build_table(bucket)
    bname = getfield(bucket, :name)
    table = getfield(bucket, :table)
    if haskey(table, name)
        return table[name]
    end
    throw(ArgumentError("'$name' not found in ConceptLookup(:$(bname))"))
end

Base.getproperty(bucket::ConceptLookup, name::Symbol) = lookup(bucket, name)

function lookup(bucket::ConceptLookup, name::AbstractString)::Int64
    lookup(bucket, normalize_concept_name(name))
end

function lookup(bucket::ConceptLookup, names::AbstractVector)
    [lookup(bucket, name) for name in names]
end

function lookup(bucket::ConceptLookup, names::Tuple)
    [lookup(bucket, name) for name in names]
end

instr(x::Union{AbstractString, Missing}, values::String...) =
        ismissing(x) ? false : coalesce(x) in values

standard_domain(row, domain_id) = 
    (row.domain_id == domain_id) && instr(row.standard_concept, "C", "S")

exclude_duplicates(row, ids) = !(row.concept_id in ids)

Race = ConceptLookup(:Race, 
    row -> standard_domain(row, "Race") && row.concept_id < 10000)
Ethnicity = ConceptLookup(:Ethnicity, 
    row -> standard_domain(row, "Ethnicity"))
ConditionStatus = ConceptLookup(:ConditionStatus,
    row -> standard_domain(row, "Condition Status"))
Specialty = ConceptLookup(:Specialty, 
    row -> standard_domain(row, "Provider") && 
           exclude_duplicates(row, (38004130, 38004142))) # NUCC pathology technician
DoseFormGroup = ConceptLookup(:DoseFormGroup,
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Dose Form Group")
ComponentClass = ConceptLookup(:ComponentClass, 
    row -> standard_domain(row, "Drug") &&
           row.concept_class_id == "Component Class")
Ingredient = ConceptLookup(:Ingredient, 
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

@funsql in_vocabulary(type, args, concept_id = :concept_id) =
    in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id, $(lookup(type, args)...)))
        select(descendant_concept_id)
    end)
