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

Base.convert(::Type{FunSQL.AbstractSQLNode}, sets::NamedConceptSets) =
    if isempty(sets.dict)
        @funsql concept().filter(false)
    else
        FunSQL.Append(args = FunSQL.SQLNode[values(sets.dict)...])
    end

function Base.show(io::IO, mime::MIME"text/html", sets::NamedConceptSets)
    df = DataFrame()
    for (var, r) in sets.dict
        df′ = DataFrame(r)
        df′[:, :variable] .= string(var)
        df = vcat(df, df′[:, [:variable, :concept_id, :vocabulary_id, :concept_code, :concept_name]])
    end
    Base.show(io, mime, _format(df, SQLFormat(limit = nothing, group_by = :variable)))
end

function handle_names_match(name)
    if startswith(name, "...") || endswith(name, "...")
		name = replace(name, "..." => "")
	elseif occursin("...", name)
		(n1, n2) = split(name, "...")
		name = [string(n1), string(n2)]
	end
    return name
end

function funsql_LOINC(code, name)
    name = handle_names_match(name)
    funsql_concept(
        funsql_assert_valid_concept(
            @funsql(vocabulary_id == "LOINC" && concept_code == $code && 
            $(isnothing(name) ? true : @funsql(icontains(concept_name, $name)))),
            :(LOINC($code, $name))))
end
        
function funsql_RxNorm(code, name)
    name = handle_names_match(name)
    funsql_concept(
        funsql_assert_valid_concept(
            @funsql(vocabulary_id == "RxNorm" && concept_code == $code && 
            $(isnothing(name) ? true : @funsql(icontains(concept_name, $name)))),
            :(RxNorm($code, $name))))
end
                
function funsql_SNOMED(code, name=nothing)
    name = handle_names_match(name)
    funsql_concept(
        funsql_assert_valid_concept(
            @funsql(vocabulary_id == "SNOMED" && concept_code == $code &&
            $(isnothing(name) ? true : @funsql(icontains(concept_name, $name)))),
            :(SNOMED($code, $name))))
end
            
function funsql_ICD10CM(code, name)
    name = handle_names_match(name)
    funsql_concept(
        funsql_assert_valid_concept(
            @funsql(vocabulary_id == "ICD10CM" && concept_code == $code && 
            $(isnothing(name) ? true : @funsql(icontains(concept_name, $name)))),
            :(ICD10CM($code, $name))))
end

function funsql_OMOP_Extension(code, name)
    name = handle_names_match(name)
    funsql_concept(
        funsql_assert_valid_concept(
            @funsql(vocabulary_id == "OMOP Extension" && concept_code == $code && 
            $(isnothing(name) ? true : @funsql(icontains(concept_name, $name)))),
                :(OMOP_Extension($code, $name))))
end
        
function funsql_CPT4(code, name)
    name = handle_names_match(name)
    funsql_concept(
        funsql_assert_valid_concept(
            @funsql(vocabulary_id == "CPT4" && concept_code == $code && 
            $(isnothing(name) ? true : @funsql(icontains(concept_name, $name)))),
            :(CPT4($code, $name))))
end
                    
@funsql begin

Type_Concept(name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "Type Concept" && concept_name == $name,
            $(:(Type_Concept($name)))))

Route(name) =
    concept(
        assert_valid_concept(
            domain_id == "Route" && concept_name == $name && standard_concept == "S",
            $(:(Route($name)))))

Provider(name) =
	concept(
		assert_valid_concept(
			domain_id == "Provider" && concept_name == $name && standard_concept == "S",
			$(:(Provider($name)))))

Visit(name) =
    concept(
        assert_valid_concept(
            domain_id == "Visit" && concept_name == $name && standard_concept == "S",
            $(:(Visit($name)))))

Dose_Form_Group(name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "RxNorm" && domain_id == "Drug" &&
            concept_class_id == "Dose Form Group" && concept_name == $name,
            $(:(Dose_Form_Group($name)))))

ConditionStatus(name) =
    concept(
        assert_valid_concept(
            domain_id == "Condition Status" && concept_name == $name && standard_concept == "S",
            $(:(ConditionStatus($name)))))

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
    visit_isa($name)

dose_form_group_isa(cs) =
    isa($cs)

dose_form_group_isa(name::AbstractString) =
    dose_form_group_isa(Dose_Form_Group($name))

condition_status_isa(cs) =
    isa(status_concept_id, $cs)

condition_status_isa(name::AbstractString) =
    condition_status_isa(ConditionStatus($name))

end

function funsql_ICD10CM(specification)
    specification = strip(replace(uppercase(specification), r"[\s,]+" => " "))
    predicate = []
    negations = []
    for chunk in split(specification, " ")
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

function funsql_isa(concept_id, concept_set)
    concept_set = convert(FunSQL.SQLNode, concept_set)
    concept_set = @funsql($concept_set.concept_descendants())
    return @funsql $concept_id in $concept_set.select(concept_id)
end

funsql_isa(concept_id, concept_set::AbstractVector) =
    @funsql isa($concept_id, append(args=$concept_set))

function funsql_isa_icd(concept_id, concept_set; with_icd9gem=false)
    concept_set = convert(FunSQL.SQLNode, concept_set)
    concept_set = @funsql($concept_set.concept_icd_descendants())
    if with_icd9gem
        concept_set = @funsql begin
            append(
                $concept_set,
                $concept_set.concept_relatives("ICD10CM - ICD9CM rev gem"))
       end
    end
    return @funsql $concept_id in $concept_set.select(concept_id)
end

funsql_isa_icd(concept_id, concept_set::AbstractVector; with_icd9gem=false) =
   @funsql isa_icd($concept_id, append(args=$concept_set); with_icd9gem=$with_icd9gem)

@funsql isa(concept_set; with_icd9gem=false) = begin
     isa(concept_id, $concept_set) ||
     if_defined_scalar(icd_concept,
        isa_icd(icd_concept.concept_id, $concept_set;
                with_icd9gem=$with_icd9gem),
        false)
end

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

# TODO: remove backward compatibility
funsql_concept_matches(cs; on = :concept_id) =
    funsql_isa(on, cs)

funsql_concept_matches(cs::Tuple{Any}; on = :concept_id) =
    funsql_concept_matches(cs[1], on = on)

funsql_concept_matches(cs::Vector; on = :concept_id) =
    funsql_concept_matches(FunSQL.Append(args = FunSQL.SQLNode[cs...]), on = on)

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
