struct NamedConceptSetsSpecification
    dict::OrderedDict{Symbol, Union{FunSQL.SQLNode, DataFrame}}
end

function funsql_concept_sets(; kws...)
    dict = OrderedDict{Symbol, FunSQL.SQLNode}()
    for (k, v) in kws
        q = v isa DataFrame ? v :
            v isa Vector{DataFrame} ? vcat(v...) :
            v isa AbstractVector ? FunSQL.Append(args = FunSQL.SQLNode[v...]) :
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

Base.convert(::Type{FunSQL.AbstractSQLNode}, sets::NamedConceptSets) =
    if isempty(sets.dict)
        @funsql concept().filter(false)
    else
        FunSQL.Append(args = FunSQL.SQLNode[values(sets.dict)...])
    end

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

struct Vocabulary
    title::String
    res::SQLResult
end

function Base.show(io::IO, voc::Vocabulary)
    print(io, "Vocabulary(")
    show(io, voc.title)
    print(io, ")")
end

(voc::Vocabulary)(args...) =
    find_concept(args..., voc)

find_concept(code, name, voc) =
    find_concept(string(code), name, voc)

function find_concept(code::String, name, voc)
    df = ensure_result!(voc.res)
    ks = findall(==(code), df.concept_code)
    isempty(ks) && throw(DomainError(code, "no concepts with the given code in $(voc.title)"))
    length(ks) > 1 && throw(DomainError(code, "more than one concept with the given code in $(voc.title)"))
    k = ks[1]
    df = df[k:k, :]
    if name isa String && occursin("...", name)
        name = pattern_to_regexp(name)
    end
    any(concept_name_matches(name), df.concept_name) || throw(DomainError(name, "concept name does not match the code in $(voc.title)"))
    df
end

function find_concept(name, voc)
    df = ensure_result!(voc.res)
    ks = findall(concept_name_matches(name), df.concept_name)
    isempty(ks) && throw(DomainError(name, "no concepts with the given name in $(voc.title)"))
    length(ks) > 1 && throw(DomainError(name, "more than one concept with the given name in $(voc.title)"))
    k = ks[1]
    df[k:k, :]
end

function concept_name_matches(concept_name::String, pattern::String)
    Unicode.isequal_normalized(pattern, concept_name, casefold = true, stripmark = true)
end

function concept_name_matches(concept_name::String, pattern::Regex)
    occursin(pattern, concept_name)
end

function concept_name_matches(pattern::String)
    if occursin("...", pattern)
        pattern = replace(pattern, r"[\\^$.[|()?*+{]" => s"\\\0")
        pattern = replace(pattern, "..." => s".+")
        pattern = replace(pattern, r"\A" => s"\\A", r"\z" => s"\\z")
        return concept_name_matches(Regex(pattern, "i"))
    end
    return Base.Fix2(concept_name_matches, pattern)
end

function concept_name_matches(pattern::Regex)
    Base.Fix2(concept_name_matches, pattern)
end

@funsql begin

ABMS_concept() =
    concept().filter(vocabulary_id == "ABMS" && is_null(invalid_reason))

ATC_concept() =
    concept().filter(vocabulary_id == "ATC" && is_null(invalid_reason))

CMS_Place_of_Service_concept() =
    concept().filter(vocabulary_id == "CMS Place of Service" && is_null(invalid_reason))

CPT4_concept() =
    concept().filter(vocabulary_id == "CPT4" && is_null(invalid_reason))

Condition_Status_concept() =
    concept().filter(vocabulary_id == "Condition Status" && is_null(invalid_reason))

HES_Specialty_concept() =
    concept().filter(vocabulary_id == "HES Specialty" && is_null(invalid_reason))

HemOnc_concept() =
    concept().filter(vocabulary_id == "HemOnc" && is_null(invalid_reason))

ICDO3_concept() =
    concept().filter(vocabulary_id == "ICDO3" && is_null(invalid_reason))

ICD10CM_concept() =
    concept().filter(vocabulary_id == "ICD10CM" && is_null(invalid_reason))

ICD10PCS_concept() =
    concept().filter(vocabulary_id == "ICD10PCS" && is_null(invalid_reason))

ICD9CM_concept() =
    concept().filter(vocabulary_id == "ICD9CM" && is_null(invalid_reason))

ICD9Proc_concept() =
    concept().filter(vocabulary_id == "ICD9Proc" && is_null(invalid_reason))

LOINC_concept() =
    concept().filter(vocabulary_id == "LOINC" && is_null(invalid_reason))

Medicare_Specialty_concept() =
    concept().filter(vocabulary_id == "Medicare Specialty" && is_null(invalid_reason))

NDFRT_concept() =
    concept().filter(vocabulary_id == "NDFRT" && is_null(invalid_reason))

NUCC_concept() =
    concept().filter(vocabulary_id == "NUCC" && is_null(invalid_reason))

None_concept() =
    concept().filter(vocabulary_id == "None" && is_null(invalid_reason))

OMOP_Extension_concept() =
    concept().filter(vocabulary_id == "OMOP Extension" && is_null(invalid_reason))

Procedure_Type_concept() =
    concept().filter(vocabulary_id == "Procedure Type" && is_null(invalid_reason))

Provider_concept() =
    concept().filter(vocabulary_id == "Provider" && is_null(invalid_reason))

Race_concept() =
    concept().filter(vocabulary_id == "Race" && is_null(invalid_reason))

RxNorm_Extension_concept() =
    concept().filter(vocabulary_id == "RxNorm Extension" && is_null(invalid_reason))

RxNorm_concept() =
    concept().filter(vocabulary_id == "RxNorm" && is_null(invalid_reason))

SNOMED_concept() =
    concept().filter(vocabulary_id == "SNOMED" && is_null(invalid_reason))

Type_Concept_concept() =
    concept().filter(vocabulary_id == "Type Concept" && is_null(invalid_reason))

UCUM_concept() =
    concept().filter(vocabulary_id == "UCUM" && is_null(invalid_reason))

Visit_concept() =
    concept().filter(vocabulary_id == "Visit" && is_null(invalid_reason))

Dose_Form_Group_concept() = begin
    concept()
    filter(
        domain_id == "Drug" &&
        vocabulary_id == "RxNorm" &&
        concept_class_id == "Dose Form Group" &&
        is_not_null(standard_concept) &&
        is_null(invalid_reason))
end

Component_Class_concept() = begin
    concept()
    filter(
        domain_id == "Drug" &&
        vocabulary_id == "HemOnc" &&
        concept_class_id == "Component Class" &&
        is_not_null(standard_concept) &&
        is_null(invalid_reason))

end

Ingredient_concept() = begin
    concept()
    filter(
        domain_id == "Drug" &&
        in(vocabulary_id, "RxNorm", "RxNorm Extension") &&
        concept_class_id == "Ingredient" &&
        is_not_null(standard_concept) &&
        is_null(invalid_reason))
end

Route_concept() = begin
    concept()
    filter(
        domain_id == "Route" &&
        vocabulary_id == "SNOMED" &&
        concept_class_id == "Qualifier Value" &&
        is_not_null(standard_concept) &&
        is_null(invalid_reason))
end

Specialty_concept() = begin
    concept()
    filter(
        domain_id == "Provider" &&
        in(vocabulary_id, "Provider", "NUCC", "HES Specialty", "Medicare Specialty", "ABMS") &&
        is_not_null(standard_concept) &&
        is_null(invalid_reason))
end

end

funsql_concept_matches(cs::Tuple{Any}, on = :concept_id, with_descendants = true) =
    funsql_concept_matches(cs[1], on = on, with_descendants = with_descendants)

funsql_concept_matches(cs::Vector; on = :concept_id, with_descendants = true) =
    funsql_concept_matches(FunSQL.Append(args = FunSQL.SQLNode[cs...]), on = on, with_descendants = with_descendants)

function funsql_concept_matches(cs; on = :concept_id, with_descendants = true)
    cs = convert(FunSQL.SQLNode, cs)
    if with_descendants
        cs = @funsql begin
            $cs
            join(from(concept_ancestor), concept_id == ancestor_concept_id)
            define(concept_id => descendant_concept_id)
        end
    end
    if on isa Symbol && !endswith(string(on), "concept_id")
        on = Symbol("$(on)_concept_id")
    end
    return @funsql $on in $cs.select(concept_id)
end

#=
function funsql_category_isa(type, cs::Union{Tuple, AbstractVector}, concept_id = :concept_id)
    cs = [c.concept_id for c in TRDW.lookup_by_name(type, cs)]
    @funsql in($concept_id, begin
        from(concept_ancestor)
        filter(in(ancestor_concept_id, $cs...))
        select(descendant_concept_id)
    end)
end

function funsql_span(cs...; join=true, icdgem=true)
    buckets = Dict{Vocabulary, Vector{Concept}}()
    for c in unnest_concept_set(cs)
        push!(get!(buckets, c.vocabulary, Concept[]), c)
    end
    qs = FunSQL.SQLNode[]
    join = join ? @funsql(as(base).join(concept(), concept_id == base.concept_id)) : @funsql(define())
    for (v, cs) in pairs(buckets)
        ids = [c.concept_id for c in cs]
        push!(qs, @funsql begin
            from(concept_ancestor)
            filter(in(ancestor_concept_id, $ids...))
            select(concept_id => descendant_concept_id)
            $join
        end)
        if v.vocabulary_id in ("ICD9CM", "ICD10CM", "ICD9Proc", "ICD10PCS", "ICD03")
            cs = ["$(c.concept_code)%" for c in cs]
            tests = build_or([@funsql(like(concept_code, $m)) for m in cs])
            push!(qs, @funsql begin
                concept()
                filter(vocabulary_id == $(v.vocabulary_id))
                filter($tests)
            end)
            # TODO: ICD9Proc - ICD10PCS gem
            if v.vocabulary_id == "ICD10CM" && icdgem
                push!(qs, @funsql begin
                    $(qs[end])
                    join(cr => begin
                        from(concept_relationship)
                        filter(relationship_id == "ICD9CM - ICD10CM gem")
                    end, cr.concept_id_2 == concept_id)
                    select(concept_id => cr.concept_id_1)
                    $join
                end)
            end
        end
    end
    length(qs) == 0 ? @funsql(concept().filter(false)) :
    length(qs) == 1 ? qs[1] :
    @funsql(append($qs...).deduplicate(concept_id))
end

function concept_matches(match...; match_on=[], span=true)
    match = unnest_concept_set(match)
    if match_on isa FunSQL.SQLNode
        match_on = [match_on]
    elseif match_on isa Symbol
        if contains(string(match_on), "concept_id")
            match_on = [match_on]
        else
            match_on = [Symbol("$(match_on)_concept_id")]
        end
    else
        if isnothing(match_on) || length(match_on) == 0
            match_on = Any[@funsql(concept_id)]
            if any([contains(c.vocabulary.vocabulary_id, "ICD") for c in match])
                push!(match_on, @funsql(ext.icd_concept_id))
            end
        end
        @assert match_on isa Vector
    end
    match = span ? funsql_span(match) : match
    parts = [ @funsql(in($col, $match.select(concept_id))) for col in match_on]
    build_or(parts)
end

concept_matches(name::Symbol, match...) =
    concept_matches(match...; match_on=name)

const funsql_concept_matches = concept_matches

function funsql_concept_in(concept_id::Symbol, ids)
    if ids isa ConceptMatchExpr
        ids = unnest_concept_ids(ids)
    end
    @funsql(filter(in($concept_id, $ids...)))
end

funsql_concept_in(q) =
    funsql_concept_in(:concept_id, q)

=#
