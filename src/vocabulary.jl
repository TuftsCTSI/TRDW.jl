struct NamedConceptSetsSpecification
    dict::OrderedDict{Symbol, FunSQL.SQLNode}
end

function funsql_concept_sets(; kws...)
    dict = OrderedDict{Symbol, FunSQL.SQLNode}()
    for (k, v) in kws
        q = v isa AbstractVector ?
            FunSQL.Append(args = FunSQL.SQLNode[v...]) :
            convert(FunSQL.SQLNode, v)
        dict[k] = q
    end
    NamedConceptSetsSpecification(dict)
end

function run(db, spec::NamedConceptSetsSpecification)
    dict = OrderedDict{Symbol, SQLResult}()
    for (k, q) in spec.dict
        dict[k] = run(db, q)
    end
    NamedConceptSets(dict)
end

struct NamedConceptSets
    dict::OrderedDict{Symbol, SQLResult}
end

Base.pairs(ncs::NamedConceptSets) =
    pairs(ncs.dict)

Base.keys(ncs::NamedConceptSets) =
    keys(ncs.dict)

Base.getindex(sets::NamedConceptSets, key::Symbol) =
    sets.dict[key]

Base.get(sets::NamedConceptSets, key::Symbol, default) =
    get(sets.dict, key, default)

FunSQL.Chain(sets::NamedConceptSets, key::Symbol) =
    sets[key]

Base.convert(type::Type{FunSQL.AbstractSQLNode}, sets::NamedConceptSets) =
    Base.convert(type, Base.convert(DataFrame, sets))

function Base.show(io::IO, mime::MIME"text/html", sets::NamedConceptSets)
    df = Base.convert(DataFrame, sets)
    df = df[:, [:concept_group, :concept_id, :vocabulary_id, :concept_code, :concept_name]]
    Base.show(io, mime, _format(df, SQLFormat(limit = nothing, group_by = :concept_group)))
end

function Base.convert(::Type{DataFrames.DataFrame}, sets::NamedConceptSets)
    df = DataFrame()
    if isempty(sets.dict)
        return DataFrame(concept_id = Integer[], concept_name = String[], domain_id = String[], vocabulary_id = String[], concept_class_id = String[], standard_concept = String[], concept_code = String[], invalid_reason = String[], concept_group = String[])
    end
    for (var, r) in sets.dict
        df′ = DataFrame(r)
        df′[:, :concept_group] .= string(var)
        df = vcat(df, df′)
    end
    return df
end

function funsql_is_concept_codename_match(code_or_name, name)
    if isnothing(name)
        match = replace(code_or_name, "..." => "%")
        return @funsql(concept_code == $code_or_name ||
                       ilike(concept_name, $match))
    else
        match = replace(name, "..." => "%")
        return @funsql(concept_code == $code_or_name &&
                       ilike(concept_name, $match))
    end
end

@funsql begin

ATC(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "ATC" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(ATC($code_or_name, $name)))))

ICD9CM(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "ICD9CM" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(ICD9CM($code_or_name, $name)))))

ICD10CM(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "ICD10CM" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(ICD10CM($code_or_name, $name)))))

RxNorm(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "RxNorm" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(RxNorm($code_or_name, $name)))))

RxNorm_Extension(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "RxNorm Extension" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(RxNorm_Extension($code_or_name, $name)))))

NDFRT(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "NDFRT" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(NDFRT($code_or_name, $name)))))

SNOMED(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "SNOMED" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(SNOMED($code_or_name, $name)))))

LOINC(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "LOINC" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(LOINC($code_or_name, $name)))))

CPT4(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "CPT4" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(CPT4($code_or_name, $name)))))

OMOP_Extension(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "OMOP Extension" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(OMOP_Extension($code_or_name, $name)))))

Type_Concept(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "Type Concept" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(Type_Concept($code_or_name, $name)))))

Route(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            domain_id == "Route" &&
            standard_concept == "S" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(Route($code_or_name, $name)))))

Provider(code_or_name, name=nothing) =
	concept(
		assert_valid_concept(
			domain_id == "Provider" &&
            standard_concept == "S" &&
            is_concept_codename_match($code_or_name, $name),
			$(:(Provider($code_or_name, $name)))))

Visit(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            domain_id == "Visit" &&
            standard_concept == "S" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(Visit($code_or_name, $name)))))

Dose_Form_Group(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "RxNorm" &&
            domain_id == "Drug" &&
            concept_class_id == "Dose Form Group" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(Dose_Form_Group($code_or_name, $name)))))

ConditionStatus(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            domain_id == "Condition Status" &&
            standard_concept == "S" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(ConditionStatus($code_or_name, $name)))))

CDM(code_or_name, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "CDM" &&
            is_concept_codename_match($code_or_name, $name),
            $(:(CDM($code_or_name, $name)))))

type_isa(cs) =
    isa(type_concept_id, $cs)

type_isa(name::AbstractString) =
    type_isa(Type_Concept($name))

route_isa(cs) =
    isa(route_concept_id, $cs)

route_isa(name::AbstractString) =
    route_isa(Route($name))

visit_isa(cs) =
    isa(if_defined_scalar(visit, visit.concept_id, concept_id), $cs)

visit_isa(name::AbstractString) =
    visit_isa(Visit($name))

dose_form_group_isa(cs) =
    isa($cs)

dose_form_group_isa(name::AbstractString) =
    dose_form_group_isa(Dose_Form_Group($name))

condition_status_isa(cs) =
    isa(status_concept_id, $cs)

condition_status_isa(name::AbstractString) =
    condition_status_isa(ConditionStatus($name))

end

function funsql_ICD10CM(; spec)
    spec = strip(replace(uppercase(spec), r"[\s,]+" => " "))
    predicate = []
    negations = []
    for chunk in split(spec, " ")
        if startswith(chunk, "-")
            push!(negations,
                @funsql(startswith(concept_code, $chunk)))
        elseif occursin("-", chunk)
            (lhs, rhs) = split(chunk, "-")
            if length(lhs) != length(rhs)
                @error("not same length $lhs - $rhs")
            end
            chunk = ""
            for n in 1:length(lhs)
                needle = lhs[1:n]
                if startswith(rhs, needle)
                    chunk = needle
                end
            end
            push!(predicate, @funsql(
                and(between(concept_code, $lhs, $rhs),
                    startswith(concept_code, $chunk),
                    length(concept_code) == length($lhs))))
        else
            if occursin("–", chunk)
                @error("mdash found in $chunk")
            end
            push!(predicate, @funsql(concept_code == $chunk))
        end
    end
    predicate = @funsql(or(args=$predicate))
    if length(negations) > 0
        negations = @funsql(or(args=$negations))
        predicate = @funsql(and($predicate, not($negations)))
    end
    @funsql begin
        concept()
        filter(vocabulary_id=="ICD10CM")
        filter($predicate)
    end
end

funsql_isa_strict(concept_id, concept_set::AbstractVector) =
    @funsql in($concept_id, append(args=$concept_set).select(concept_id))

function funsql_define_isa(ncs::NamedConceptSets; with_icd9gem=false)
    query = @funsql(define())
    for (name, cset) in pairs(ncs)
        name = Symbol("isa_$name")
        query = @funsql begin
            $query
            define($name => isa($cset; with_icd9gem = $with_icd9gem))
        end
    end
    return query
end

function funsql_concept_sets_breakout(pair::Pair{Symbol, NamedConceptSets}; with_icd9gem=false)
    (colname, ncs) = pair
    if length(ncs.dict) < 1
        return @funsql(define($colname => missing))
    end
    df = DataFrame(:label => collect([string(k) for k in keys(ncs.dict)]))
    frame = :_concept_sets_breakout
    args = [@funsql($frame.label)]
    for (name, cset) in pairs(ncs)
        push!(args, @funsql($(string(name))))
        push!(args, @funsql(isa($cset; with_icd9gem = $with_icd9gem)))
    end
    push!(args, @funsql(false))
    @funsql begin
        left_join($frame => from($df), decode($args...))
        define($colname => $frame.label)
    end
end

function print_concepts(df; prefix="        ")
    df = DataFrame(df)
    first = true
    sort!(df, [:vocabulary_id, :concept_name])
    for row in eachrow(df)
        !first && println(",")
        print(prefix)
        print(replace(row.vocabulary_id, " " => "_"))
        print("(\"$(row.concept_code)\", \"$(row.concept_name)\")")
        first = false
    end
    println()
end
