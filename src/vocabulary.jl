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

SNOMED(code, name) =
    concept(
        assert_valid_concept(
            vocabulary_id == "SNOMED" && concept_code == $code && concept_name == $name,
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

isa(cs; with_descendants = true) =
    isa(concept_id, $cs, with_descendants = $with_descendants)

type_isa(cs; with_descendants = true) =
    isa(type_concept_id, $cs, with_descendants = $with_descendants)

type_isa(name::AbstractString; with_descendants = true) =
    type_isa(Type_Concept($name), with_descendants = $with_descendants)

dose_form_group_isa(cs; with_descendants = true) =
    isa($cs, with_descendants = $with_descendants)

dose_form_group_isa(name::AbstractString; with_descendants = true) =
    dose_form_group_isa(Dose_Form_Group($name), with_descendants = $with_descendants)

end

function funsql_isa(concept_id, concept_set; with_descendants = true)
    concept_set = convert(FunSQL.SQLNode, concept_set)
    if with_descendants
        concept_set = @funsql begin
            $concept_set
            join(from(concept_ancestor), concept_id == ancestor_concept_id)
            define(concept_id => descendant_concept_id)
        end
    end
    return @funsql $concept_id in $concept_set.select(concept_id)
end

funsql_concept_matches(cs; on = :concept_id, with_descendants = true) =
    funsql_isa(on, cs, with_descendants = with_descendants)

funsql_concept_matches(cs::Tuple{Any}; on = :concept_id, with_descendants = true) =
    funsql_concept_matches(cs[1], on = on, with_descendants = with_descendants)

funsql_concept_matches(cs::Vector; on = :concept_id, with_descendants = true) =
    funsql_concept_matches(FunSQL.Append(args = FunSQL.SQLNode[cs...]), on = on, with_descendants = with_descendants)

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
