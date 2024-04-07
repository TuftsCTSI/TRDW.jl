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

Base.pairs(ncs::NamedConceptSets) = pairs(ncs.dict)

Base.convert(::Type{FunSQL.AbstractSQLNode}, sets::NamedConceptSets) =
    if isempty(sets.dict)
        @funsql concept().filter(false)
    else
        FunSQL.Append(args = FunSQL.SQLNode[values(sets.dict)...])
    end

FunSQL.Chain(ncs::NamedConceptSets, name::Symbol) = ncs.dict[name]

function Base.show(io::IO, m::MIME"text/html", sets::NamedConceptSets)
    print(io, """
    <div style="overflow:scroll;max-height:500px;">
    <table><tr><th><i>variable</i></th>
    <th>concept_id</th>
    <th>vocabulary_id</th>
    <th>concept_code</th>
    <th>concept_name</th></tr>
    """)
    for (k, r) in sets.dict
        df = ensure_result!(r)
        len = size(df, 1)
        foreach(
            1:len,
            df.concept_id,
            df.vocabulary_id,
            df.concept_code,
            df.concept_name) do n, concept_id, vocabulary_id, concept_code, concept_name
                show(io, m,
                @htl("""
                  <tr>
                      $(n==1 ? @htl("""
                              <td rowspan=$len
                                  style=$(len > 1 ?
                                          @htl("vertical-align: top") : "")>
                              <i>$k</i></td>
                          """) : "")
                      <td>$concept_id</td>
                      <td>$vocabulary_id</td>
                      <td>$concept_code</td>
                      <td>$concept_name</td>
                  </tr>
                """))
        end
    end
    print(io, "</table></div>")
end

Base.getindex(sets::NamedConceptSets, key::Symbol) =
    sets.dict[key]

Base.get(sets::NamedConceptSets, key::Symbol, default) =
    get(sets.dict, key, default)

FunSQL.Chain(sets::NamedConceptSets, key::Symbol) =
    sets[key]

@funsql begin

LOINC(code, name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "LOINC" && concept_code == $code && concept_name == $name,
            $(:(LOINC($code, $name)))))

RxNorm(code, name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "RxNorm" && concept_code == $code && concept_name == $name,
            $(:(RxNorm($code, $name)))))

SNOMED(code, name=nothing) =
    concept(
        assert_valid_concept(
            vocabulary_id == "SNOMED" && concept_code == $code &&
            $(isnothing(name) ? true : @funsql(concept_name == $name)),
            $(:(SNOMED($code, $name)))))

ICD10CM(code, name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "ICD10CM" && concept_code == $code && concept_name == $name,
            $(:(ICD10CM($code, $name)))))

Type_Concept(name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "Type Concept" && concept_name == $name,
            $(:(Type_Concept($name)))))

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

type_isa(cs; with_descendants = true) =
    isa(type_concept_id, $cs, with_descendants = $with_descendants)

type_isa(name::AbstractString; with_descendants = true) =
    type_isa(Type_Concept($name), with_descendants = $with_descendants)

dose_form_group_isa(cs; with_descendants = true) =
    isa($cs, with_descendants = $with_descendants)

dose_form_group_isa(name::AbstractString; with_descendants = true) =
    dose_form_group_isa(Dose_Form_Group($name), with_descendants = $with_descendants)

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

function funsql_isa(concept_id, concept_set)
    concept_set = convert(FunSQL.SQLNode, concept_set)
    concept_set = @funsql($concept_set.concept_descendants())
    return @funsql $concept_id in $concept_set.select(concept_id)
end

function funsql_isa_icd(concept_id, concept_set; with_icd9to10gem=false)
    concept_set = convert(FunSQL.SQLNode, concept_set)
    concept_set = @funsql($concept_set.concept_icd_descendants())
    if with_icd9to10gem
        concept_set = @funsql begin
            append(
                $concept_set,
                $concept_set.concept_relatives("ICD10CM - ICD9CM rev gem"))
       end
    end
    return @funsql $concept_id in $concept_set.select(concept_id)
end

@funsql isa(concept_set; with_icd9to10gem=false) = begin
     isa(concept_id, $concept_set) ||
     if_defined_scalar(icd_concept,
        isa_icd(icd_concept.concept_id, $concept_set;
                with_icd9to10gem=$with_icd9to10gem),
        true)
end

function funsql_define_isa(ncs::NamedConceptSets; with_icd9to10gem=false)
    query = @funsql(define())
    for (name, cset) in pairs(ncs)
        name = Symbol("isa_$name")
        query = @funsql begin
            $query
            define($name => isa($cset; with_icd9to10gem = $with_icd9to10gem))
        end
    end
    return query
end

# TODO: remove backward compatibility
funsql_concept_matches(cs; on = :concept_id, with_descendants = true) =
    funsql_isa(on, cs, with_descendants = with_descendants)

funsql_concept_matches(cs::Tuple{Any}; on = :concept_id, with_descendants = true) =
    funsql_concept_matches(cs[1], on = on, with_descendants = with_descendants)

funsql_concept_matches(cs::Vector; on = :concept_id, with_descendants = true) =
    funsql_concept_matches(FunSQL.Append(args = FunSQL.SQLNode[cs...]), on = on,
                           with_descendants = with_descendants)
