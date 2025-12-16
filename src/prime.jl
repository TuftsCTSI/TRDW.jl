module Prime


using ..TRDW: TRDW
export TRDW

using FunSQL


"""
    @funsql switch(test::Bool, q, [else_q])

Conditionally apply a combinator based on the value of the `test` parameter.

# Examples

```julia
@funsql valid_concept(; standard_only = true) = begin
    concept()
    filter(is_null(invalid_reason))
    switch(
        \$standard_only,
        filter(is_not_null(standard_concept)))
end
```
"""
@funsql switch(test::Bool, q, else_q = define()) =
    $(test ? q : else_q)

export funsql_switch


function funsql_equals_or_in
end

@funsql equals_or_in(l, r) =
    $l == $r

@funsql equals_or_in(l, r::AbstractVector) =
    in($l, $(r...))

@funsql equals_or_in(l, r::Nothing) =
    true

export funsql_equals_or_in


"""
    @funsql concept()
    @funsql concept(concept_ids...)

Return records from the `CONCEPT` table:
- Without arguments, return all records;
- With one or more `concept_ids`, return records where `CONCEPT.CONCEPT_ID`
  matches the given values.

For clarity, some `CONCEPT` columns are omitted from the output.

# Examples

```julia
@funsql Male() =
    concept(8507)
```
"""
function funsql_concept
end

@funsql concept() = begin
    from(concept)
    define(base_concept_id => concept_id, after = concept_id)
    define(
        valid_start_date,
        valid_end_date,
        private = true)
end

@funsql concept(concept_ids...) = begin
    concept()
    filter(in(concept_id, $(concept_ids...)))
end

export funsql_concept


"""
    @funsql matches_concept(code_or_name)
    @funsql matches_concept(code, name)

Evaluate whether the input `CONCEPT` record matches the given patterns:
- With a single argument, returns `true` if either `CONCEPT_CODE` or `CONCEPT_NAME`
  matches the pattern.
- With two arguments, returns `true` if both `CONCEPT_CODE` and `CONCEPT_NAME`
  match their respective patterns.

Code matching is exact.  Name matching is case-insensitive, and the pattern may
include wildcards `%`, `…`, or `...`.

# Examples

```julia
@funsql Gender(code_or_name, name = nothing) = begin
    concept()
    filter(
        vocabulary_id == "Gender" &&
        matches_concept(\$code_or_name, \$name))
end

@funsql Gender("F")

@funsql Gender("Female")

@funsql Gender("F", "Female")

@funsql Gender("Fem…")
```
"""
function funsql_matches_concept(code_or_name, name = nothing)
    name_pattern = replace(something(name, code_or_name), "…" => "%", "..." => "%")
    if name === nothing
        @funsql concept_code == $code_or_name || ilike(concept_name, $name_pattern)
    else
        @funsql concept_code == $code_or_name && ilike(concept_name, $name_pattern)
    end
end

export funsql_matches_concept


"""
    @funsql include_descendant_concepts()

Add descendant concepts to the input concept set.

# Examples

```julia
@funsql Hypertension() = begin
    concept(316866 #= Hypertensive disorder =#)
    include_descendant_concepts()
end
```
"""
@funsql include_descendant_concepts() = begin
    into(base, private = true)
    left_join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.ancestor_concept_id,
        private = true)
    join(
        concept => concept(),
        coalesce(concept_ancestor.descendant_concept_id, base.concept_id) == concept.concept_id,
        optional = true,
        private = true)
    define(
        concept_id => coalesce(concept_ancestor.descendant_concept_id, base.concept_id),
        base_concept_id => base.base_concept_id,
        concept.concept_name,
        concept.domain_id,
        concept.vocabulary_id,
        concept.concept_class_id,
        concept.standard_concept,
        concept.concept_code,
        concept.invalid_reason)
end

export funsql_include_descendant_concepts


"""
    @funsql include_prefix_descendant_concepts()

Add descendant concepts to the input concept set using prefix-based matching
on the concept code.  It is intended to use with vocabularies such as ICD10CM,
where the concept code lexically encodes hierarchical relationships.

# Examples

```julia
@funsql Hypertension() = begin
    concept(1569133 #= I25 Chronic ischemic heart disease =#)
    include_prefix_descendant_concepts()
end
```
"""
@funsql include_prefix_descendant_concepts() = begin
    into(base, private = true)
    join(
        concept => concept(),
        base.vocabulary_id == concept.vocabulary_id && startswith(concept.concept_code, base.concept_code),
        private = true)
    define(
        concept_id => concept.concept_id,
        base_concept_id => base.base_concept_id,
        concept.concept_name,
        concept.domain_id,
        concept.vocabulary_id,
        concept.concept_class_id,
        concept.standard_concept,
        concept.concept_code,
        concept.invalid_reason)
end

export funsql_include_prefix_descendant_concepts


"""
    @funsql map_to_related_concepts(relationship_id)

For each input concept, emit related concepts as defined by the
specified `relationship_id`.

# Examples
```julia
@funsql begin
    concept()
    filter(vocabulary_id == "ICD10CM" && concept_code == "I10")
    map_to_related_concepts("Maps to")
end
```
"""
@funsql map_to_related_concepts(relationship_id) = begin
    into(base, private = true)
    join(
        concept_relationship => begin
            from(concept_relationship)
            filter(relationship_id == $relationship_id)
        end,
        base.concept_id == concept_relationship.concept_id_1,
        private = true)
    join(
        concept => concept(),
        concept_relationship.concept_id_2 == concept.concept_id,
        optional = true,
        private = true)
    define(
        concept_id => concept_relationship.concept_id_2,
        base_concept_id => base.base_concept_id,
        concept.concept_name,
        concept.domain_id,
        concept.vocabulary_id,
        concept.concept_class_id,
        concept.standard_concept,
        concept.concept_code,
        concept.invalid_reason)
end

export funsql_map_to_related_concepts


"""
    @funsql map_to_standard_concepts(domain_id = nothing)

Map each input concept to the corresponding standard concepts,
optionally restricting them by the target domain.

# Examples
```julia
@funsql begin
    concept()
    filter(vocabulary_id == "ICD10CM" && concept_code == "I10")
    map_to_standard_concepts("Condition")
end
```
"""
@funsql map_to_standard_concepts(domain_id = nothing) = begin
    map_to_related_concepts("Maps to")
    filter(equals_or_in(domain_id, $domain_id))
end

export funsql_map_to_standard_concepts


"""
    @funsql include_concepts(concept_set)

Add the given concept set to the input concept set.  With no input, return
the given concept set.  Commonly used for defining composable vocabulary
combinators.

# Examples

```julia
@funsql SNOMED(code_or_name, name = nothing) =
    include_concepts(
        begin
            concept()
            filter(
                vocabulary_id == "SNOMED" &&
                matches_concept(\$code_or_name, \$name))
            include_descendant_concepts()
        end)

@funsql begin
    SNOMED("195967001", "Asthma")
    SNOMED("233604007", "Pneumonia")
end
```
"""
@funsql include_concepts(concept_set) =
    append($concept_set)

export funsql_include_concepts


"""
    @funsql exclude_concepts(concept_set)

Remove the given concept set from the input concept set.

# Examples

```julia
@funsql begin
    SNOMED("73211009", "Diabetes mellitus")
    exclude_concepts(SNOMED("11687002", "Gestational diabetes mellitus"))
end
```
"""
@funsql exclude_concepts(concept_set) = begin
    left_join(
        _excluded_concepts => $concept_set.group(concept_id),
        concept_id == _excluded_concepts.concept_id,
        private = true)
    filter(is_null(_excluded_concepts.concept_id))
end

export funsql_exclude_concepts


"""
    @funsql intersect_concepts(concept_set)

Restrict the input concept set to concepts that also appear in the given concept
set.

# Examples

```julia
@funsql Chronic_Back_Pain() = begin
    SNOMED("82423001", "Chronic pain")
    intersect_concepts(SNOMED("161891005", "Backache"))
end
```
"""
@funsql intersect_concepts(concept_set) =
    join(
        _intersected_concepts => $concept_set.group(concept_id),
        concept_id == _intersected_concepts.concept_id,
        private = true)

export funsql_intersect_concepts


"""
    @funsql deduplicate_concepts()

Remove duplicate concept records from the input concept set.
"""
@funsql deduplicate_concepts() = begin
    partition(name = _deduplicate_concepts, by = [concept_id], order_by = [missing])
    filter(_deduplicate_concepts.row_number() <= 1)
    define(base_concept_id => concept_id)
    undefine(_deduplicate_concepts)
end

export funsql_deduplicate_concepts


function funsql_concept_spec_to_predicate(name, spec, shared)
    args = []
    for item in split(spec, r"\s*,\s*", keepempty = false)
        codes = split(item, r"\s*[-–]\s*")
        @assert length(codes) == 1 || length(codes) == 2 && length(codes[1]) == length(codes[2]) && codes[1] < codes[2] "invalid concept spec item: $item"
        if length(codes) == 1
            code = codes[1]
            push!(args, @funsql assert_valid_concept($shared && concept_code == $code, $(:($name($code)))))
        else
            code1, code2 = codes
            l = length(code1)
            push!(args, @funsql assert_valid_concept($shared && concept_code == $code1, $(:($name($code1)))))
            push!(args, @funsql $shared && $code1 < concept_code < $code2 && length(concept_code) == $l)
            push!(args, @funsql assert_valid_concept($shared && concept_code == $code2, $(:($name($code2)))))
        end
    end
    @funsql or(args = $args)
end


"""
    @funsql Provenance(code, name, descend = true)
    @funsql Provenance(code_or_name, descend = true)

Return a concept set representing a record provenance.

Arguments must refer to a concept from the standard OMOP *Type Concept* vocabulary.

# Examples

```julia
@funsql Provenance("EHR")
```
"""
@funsql Provenance(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Type Concept" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Type_Concept($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_Provenance


"""
    @funsql Care_Site(code_or_name; descend = true)
    @funsql Care_Site(code, name; descend = true)

Generate a concept set for care sites.

Arguments should refer to a concept within care site concept hierarchy,
which includes locations (sites), departments, and department specialties.

# Examples

```julia
@funsql TMC() =
    Care_Site("Tufts Medical Center Parent")
```

```julia
@funsql Emergency_Department() =
    Care_Site("Emergency Medicine")
```
"""
@funsql Care_Site(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    in(vocabulary_id, "clarity_loc", "clarity_dep", "zc_specialty_dep") &&
                        matches_concept($code_or_name, $name),
                    $(:(CareSite($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_Care_Site


"""
    @funsql Provider(code_or_name; descend = true)
    @funsql Provider(code, name; descend = true)

Generate a concept set for provider's type or specialty.

Arguments should refer to a standard concept within the Provider domain, which
includes provider types such as Physician or Registered Nurse and specialties
such as Cardiology or Rheumatology.

# Examples

```julia
@funsql Nurse() =
    Provider("Nurse")
```

```julia
@funsql Rheumatologist() =
    Provider("Rheumatology")
```
"""
@funsql Provider(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    domain_id == "Provider" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Provider($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_Provider


"""
    @funsql Gender()

Return all concepts representing patient sex.
"""
@funsql Gender() =
    include_concepts(
        concept(
            8507 #= MALE =#,
            8532 #= FEMALE =#))

"""
    @funsql Gender(code, name)
    @funsql Gender(code_or_name)

Return a concept representing patient sex.

Arguments must refer to a concept from the standard OMOP *Gender* vocabulary.

# Examples

```julia
@funsql Gender("M")
```
"""
@funsql Gender(code_or_name, name = nothing) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Gender" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Gender($code_or_name, $name)))))
        end)

export funsql_Gender


"""
    @funsql Race()

Return all concepts representing patient race per OMB standard.
"""
@funsql Race() =
    include_concepts(
        concept(
            8515 #= Asian =#,
            8516 #= Black or African American =#,
            8527 #= White =#,
            8557 #= Native Hawaiian or Other Pacific Islander =#,
            8657 #= American Indian or Alaska Native =#,
            1546847 #= More than one race =#))

"""
    @funsql Race(code, name)
    @funsql Race(code_or_name)

Return a concept representing patient race.

Arguments must refer to a concept from the standard OMOP *Race* vocabulary.

# Examples

```julia
@funsql Race("Asian")
```
"""
@funsql Race(code_or_name, name = nothing) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Race" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Race($code_or_name, $name)))))
        end)

export funsql_Race


"""
    @funsql Ethnicity()

Return all concepts representing patient ethnicity per OMB standard.
"""
@funsql Ethnicity() =
    include_concepts(
        concept(
            38003563 #= Hispanic or Latino =#,
            38003564 #= Not Hispanic or Latino =#))

"""
    @funsql Ethnicity(code, name)
    @funsql Ethnicity(code_or_name)

Return a concept representing patient ethnicity.

Arguments must refer to a concept from the standard OMOP *Ethnicity* vocabulary.

# Examples

```julia
@funsql Ethnicity("Hispanic or Latino")
```
"""
@funsql Ethnicity(code_or_name, name = nothing) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Ethnicity" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Ethnicity($code_or_name, $name)))))
        end)

export funsql_Ethnicity


"""
    @funsql Visit()

Return all concepts representing the visit type in TRDW.
"""
@funsql Visit() =
    include_concepts(
        concept(
            262 #= Emergency Room and Inpatient Visit =#,
            9201 #= Inpatient Visit =#,
            9202 #= Outpatient Visit =#,
            9203 #= Emergency Room Visit =#,
            581476 #= Home Visit =#,
            722455 #= Telehealth =#))

"""
    @funsql Visit(code, name)
    @funsql Visit(code_or_name)

Return a concept representing the visit type.

Arguments must refer to a concept from the standard OMOP *Visit* vocabulary.

# Examples

```julia
@funsql IP() = begin
    Visit("IP")
    Visit("ERIP")
end
```
"""
@funsql Visit(code_or_name, name = nothing) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Visit" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Visit($code_or_name, $name)))))
        end)

export funsql_Visit


"""
    @funsql SNOMED(code_or_name; descend = true)
    @funsql SNOMED(code, name; descend = true)

Generate a SNOMED concept set.

Arguments should refer to a standard concept in the SNOMED vocabulary.

# Examples

```julia
@funsql Hypertension() =
    SNOMED("59621000", "Essential hypertension")
```
"""
@funsql SNOMED(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "SNOMED" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(SNOMED($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_SNOMED


"""
    @funsql ICD10CM(code_or_name; descend = true)
    @funsql ICD10CM(code, name; descend = true)

Generate an ICD10CM concept set.

Arguments should refer to a concept in the ICD10CM vocabulary.

# Examples

```julia
@funsql Hypertension() =
    ICD10CM("I10", "Essential (primary) hypertension")
```
"""
@funsql ICD10CM(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "ICD10CM" &&
                        is_null(invalid_reason) &&
                        matches_concept($code_or_name, $name),
                    $(:(ICD10CM($code_or_name, $name; descend = $descend)))))
            switch($descend, include_prefix_descendant_concepts())
        end)

"""
    @funsql ICD10CM(; spec, descend = true)

Generate an ICD10CM concept set from the specification string.

The `spec` string is a comma-separated list of ICD10CM codes (e.g. H34.0)
or code ranges (e.g., I60-I69).

# Examples

```julia
@funsql Cerebrovascular_Disease() =
    ICD10CM(spec = "I60-I69, G45, G46, H34.0")
```
"""
@funsql ICD10CM(; spec, descend = true) =
    include_concepts(
        begin
            concept()
            filter(concept_spec_to_predicate(ICD10CM, $spec, vocabulary_id == "ICD10CM" && is_null(invalid_reason)))
            switch($descend, include_prefix_descendant_concepts())
        end)

export funsql_ICD10CM


"""
    @funsql Condition_Status(code, name, descend = true)
    @funsql Condition_Status(code_or_name, descend = true)

Return a concept set representing the condition status.

Arguments must refer to a concept from the standard OMOP *Condition Status* vocabulary.

# Examples

```julia
@funsql Condition_Status("Primary discharge diagnosis")
```
"""
@funsql Condition_Status(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Condition Status" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Type_Concept($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_Condition_Status


"""
    @funsql RxNorm(code, name, descend = true)
    @funsql RxNorm(code_or_name, descend = true)

Return a concept set representing a drug defined in the RxNorm vocabulary.

Arguments must refer to a standard concept from the OMOP *RxNorm* and
*RxNorm Extension* vocabularies.

# Examples

```julia
@funsql RxNorm("acetaminophen")
```
"""
@funsql RxNorm(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    in(vocabulary_id, "RxNorm", "RxNorm Extension") &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(RxNorm($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_RxNorm


"""
    @funsql CVX(code, name, descend = true)
    @funsql CVX(code_or_name, descend = true)

Return a concept set representing a vaccine.

Arguments must refer to a standard concept from the OMOP *CVX* vocabulary.

# Examples

```julia
@funsql CVX("influenza virus vaccine, unspecified formulation")
```
"""
@funsql CVX(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "CVX" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(CVX($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_CVX


"""
    @funsql Route(code_or_name; descend = true)
    @funsql Route(code, name; descend = true)

Generate a concept set representing medication administration route.

Arguments should refer to a standard concept within the *Route* domain.

# Examples

```julia
@funsql Route("Intravenous")
```
"""
@funsql Route(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    domain_id == "Route" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(Route($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_Route


"""
    @funsql UCUM(code_or_name)
    @funsql UCUM(code, name)

Generate a concept set representing a unit of measurement.

Arguments should refer to a standard concept within the *UCUM* vocabulary.

# Examples

```julia
@funsql UCUM("mg")
```
"""
@funsql UCUM(code_or_name, name = nothing) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "UCUM" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(UCUM($code_or_name, $name)))))
        end)

export funsql_UCUM


"""
    @funsql CPT4(code, name, descend = true)
    @funsql CPT4(code_or_name, descend = true)

Return a concept set representing a CPT4 procedure code.

Arguments must refer to a concept from the OMOP *CPT4* vocabulary.

# Examples

```julia
@funsql CPT4("1013012", "Electrocardiogram, routine ECG with at least 12 leads")
```
"""
@funsql CPT4(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "CPT4" &&
                        is_null(invalid_reason) &&
                        matches_concept($code_or_name, $name),
                    $(:(CPT4($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_CPT4


"""
    @funsql HCPCS(code, name, descend = true)
    @funsql HCPCS(code_or_name, descend = true)

Return a concept set representing a HCPCS procedure code.

Arguments must refer to a concept from the OMOP *HCPCS* vocabulary.

# Examples

```julia
@funsql HCPCS("A4615", "Cannula, nasal")
```
"""
@funsql HCPCS(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "HCPCS" &&
                        is_null(invalid_reason) &&
                        matches_concept($code_or_name, $name),
                    $(:(HCPCS($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_HCPCS


"""
    @funsql LOINC(code, name, descend = true)
    @funsql LOINC(code_or_name, descend = true)

Return a concept set representing a clinical or laboratory observation.

Arguments must refer to a standard concept from the OMOP *LOINC* vocabulary.

# Examples

```julia
@funsql LOINC("Body mass index (BMI) [Ratio]")
```
"""
@funsql LOINC(code_or_name, name = nothing; descend = true) =
    include_concepts(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "LOINC" &&
                        is_not_null(standard_concept) &&
                        matches_concept($code_or_name, $name),
                    $(:(LOINC($code_or_name, $name; descend = $descend)))))
            switch($descend, include_descendant_concepts())
        end)

export funsql_LOINC


"""
    @funsql has(name)

Return a Boolean value indicating whether the joined subquery referenced by
`name` contains at least one row.  It is typically used after `left_join()`
or an attach-like combinator.

# Examples
```julia
@funsql begin
    from(condition_occurrence)
    left_join(
        visit => from(visit_occurrence),
        visit.visit_occurrence_id == visit_occurrence_id)
    define(has_visit => has(visit))
end
```
"""
@funsql has(name) =
    coalesce($name.true, false)

export funsql_has


"""
    @funsql join_first(joinee, on; order_by, left = false, private = false)
    @funsql join_first(joinee; on, order_by, left = false, private = false)
    @funsql left_join_first(joinee, on; order_by, private = false)
    @funsql left_join_first(joinee; on, order_by, private = false)

Join each input row to the first matching row from `joinee`, where the order
of `joinee` rows is determined by the specified `order_by` list.  Unlike
a standard `join()`, `join_first()` guarantees that the output cardinality
does not increase.

# Examples

```julia
@funsql begin
    person()
    left_join_first(
        first_visit => visit(),
        person_id == first_visit.person_id,
        order_by = [first_visit.datetime])
end
```
"""
function join_first
end

@funsql join_first(joinee, on; order_by, left = false, private = false) =
    join_first(joinee; on = $on, order_by = $order_by, left = $left, private = $private)

@funsql join_first(joinee; on, order_by, left = false, private = false) = begin
    partition(order_by = [missing], name = _join_first_1)
    join($joinee, $on, left = $left, private = $private)
    partition(_join_first_1.row_number(), order_by = $order_by, name = _join_first_2)
    filter(_join_first_2.row_number() <= 1)
    undefine(_join_first_1, _join_first_2)
end

export funsql_join_first

@funsql left_join_first(joinee, on; order_by, private = false) =
    join_first($joinee, on = $on, order_by = $order_by, left = true, private = $private)

@funsql left_join_first(joinee; on, order_by, private = false) =
    join_first($joinee, on = $on, order_by = $order_by, left = true, private = $private)

export funsql_left_join_first


"""
    @funsql attach_first(joinee, on = true; by = [person_id], order_by, private = false)

Join each input row to the first matching row from `joinee` that has the same
`person_id` (or another key specified in `by`) and satisfies the optional
`on` predicate.  The order of `joinee` rows is determined by the specified
`order_by` list.  Unlike `join()`, `attach_first()` does not eliminate or
duplicate input rows.

# Examples

```julia
@funsql begin
    person()
    attach_first(
        first_visit => visit(),
        person_id == first_visit.person_id,
        order_by = [first_visit.datetime])
end
"""
@funsql attach_first(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id], order_by, private = false) =
    left_join_first($joinee, on = and(args = $[@funsql($key == $joinee_name.$key) for key in by]) && $on, order_by = $order_by, private = $private)

export funsql_attach_first

"""
    @funsql attach_earliest(joinee, on = true; by = [person_id], private = false)

Join each input row to the earliest matching row from `joinee` (determined by
the `datetime` attribute) that has the same `person_id` (or another key
specified in `by`) and satisfies the optional `on` predicate.

# Examples

```julia
@funsql begin
    person()
    attach_earliest(first_visit => visit())
end
"""
@funsql attach_earliest(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id], private = false) =
    attach_first($joinee, $on; by = $by, order_by = [$joinee_name.datetime.asc(nulls = last)], private = $private)

export funsql_attach_earliest


"""
    @funsql attach_latest(joinee, on = true; by = [person_id], private = false)

Join each input row to the latest matching row from `joinee` (determined by
the `datetime` attribute) that has the same `person_id` (or another key
specified in `by`) and satisfies the optional `on` predicate.

# Examples

```julia
@funsql begin
    person()
    attach_latest(latest_visit => visit())
end
"""
@funsql attach_latest(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id], private = false) =
    attach_first($joinee, $on; by = $by, order_by = [$joinee_name.datetime.desc(nulls = last)], private = $private)

export funsql_attach_latest


"""
    @funsql attach_nearest(joinee, on = true; by = [person_id], private = false)

Join each input row to the temporally closest matching row from `joinee`
(determined by the absolute difference between `datetime` attributes)
that has the same `person_id` (or another key specified in `by`) and
satisfies the optional `on` predicate.

# Examples

```julia
@funsql begin
    visit()
    attach_nearest(
        bmi => measurement(LOINC("39156-5")),
        dateadd_day(-30, datetime) ≤ bmi.datetime ≤ dateadd_day(30, datetime))
end
"""
@funsql attach_nearest(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id], private = false) =
    attach_first($joinee, $on; by = $by, order_by = [abs(datediff_second(datetime, $joinee_name.datetime)).asc(nulls = last)], private = $private)

export funsql_attach_nearest


"""
    @funsql attach_group(joinee, by = [person_id], private = false)

Join each input row to the grouped rows from `joinee` that have the same
`person_id` (or another key specified in `by`).

# Examples

```julia
@funsql begin
    person()
    attach_group(visits => visit())
    define(n_visit => coalesce(visits.count(), 0))
end
```
"""
@funsql attach_group(joinee; joinee_name = $(FunSQL.label(joinee)), by = [person_id], private = false) =
    left_join(
        $joinee.group(by = $by),
        on = and(args = $[@funsql($key == $joinee_name.$key) for key in by]),
        private = $private)

export funsql_attach_group

"""
    @funsql attach_all(joinee, on = true; by = [person_id], name = nothing, private = false)

TODO: semantics and explanation
"""
@funsql attach_all(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id], name = nothing, private = false) =
    begin
        partition(order_by = [missing], name = _attach_all)
        left_join(
            $joinee,
            on = and(args = $[@funsql($key == $joinee_name.$key) for key in by]) && $on,
            private = $private)
        partition(_attach_all.row_number(), order_by = [missing], name = $name)
        filter($name.row_number() <= 1)
        undefine(_attach_all, $joinee_name)
    end

export funsql_attach_all


"""
    @funsql filter_with(joinee, on = true; by = [person_id])

Filter the input to rows that have at least one matching row in `joinee` with
the same `person_id`, optionally resticted by the `on` predicate.

# Example

```julia
@funsql begin
    person()
    filter_with(condition(SNOMED("Essential hypertension")))
end
```
"""
@funsql filter_with(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id]) =
    switch(
        $(on === true),
        begin
            join(
                $joinee.group(by = $by),
                on = and(args = $[@funsql($key == $joinee_name.$key) for key in by]))
            undefine($joinee_name)
        end,
        begin
            partition(order_by = [missing], name = _filter_with_1)
            join(
                $joinee,
                on = and(args = $[@funsql($key == $joinee_name.$key) for key in by]) && $on)
            partition(_filter_with_1.row_number(), order_by = [missing], name = _filter_with_2)
            filter(_filter_with_2.row_number() <= 1)
            undefine(_filter_with_1, _filter_with_2, $joinee_name)
        end)

export funsql_filter_with


"""
    @funsql filter_without(joinee, on = true; by = [person_id])

Filter the input to rows that have no matching rows in `joinee` with the same
`person_id`, optionally resticted by the `on` predicate.

# Example

```julia
@funsql begin
    person()
    filter_without(condition(SNOMED("Essential hypertension")))
end
```
"""
@funsql filter_without(joinee, on = true; joinee_name = $(FunSQL.label(joinee)), by = [person_id]) = begin
    left_join(
        $joinee,
        on = and(args = $[@funsql($key == $joinee_name.$key) for key in by]) && $on)
    filter(is_null($joinee_name.true))
    undefine($joinee_name)
end

export funsql_filter_without


"""
    @funsql take_first(; by = [person_id], order_by)

For each person, keep the first record according to `order_by`.

# Examples

```julia
@funsql begin
    visit()
    take_first(order_by = [datetime])
end
```
"""
@funsql take_first(; by = [person_id], order_by) = begin
    partition(by = $by, order_by = $order_by, name = _take_first)
    filter(_take_first.row_number() <= 1)
    undefine(_take_first)
end

export funsql_take_first


"""
    @funsql take_earliest(; by = [person_id])

For each person, keep the earliest record based on `datetime`.

# Examples

```julia
@funsql begin
    visit()
    take_earliest()
end
```
"""
@funsql take_earliest(; by = [person_id]) =
    take_first(by = $by, order_by = [datetime.asc(nulls = last)])

export funsql_take_earliest


"""
    @funsql take_latest(; by = [person_id])

For each person, keep the latest record based on `datetime`.

# Examples

```julia
@funsql begin
    visit()
    take_latest()
end
```
"""
@funsql take_latest(; by = [person_id]) =
    take_first(by = $by, order_by = [datetime.desc(nulls = last)])

export funsql_take_latest


"""
    @funsql define_era(datetime, end_datetime; by = [person_id], name = era, before = nothing, after = nothing, private = false)

Define an `era` identifier that labels overlapping or contiguous rows using
the time range from `datetime` to `end_datetime`.

# Examples

```julia
@funsql begin
    drug(RxNorm("ampicillin"))
    filter(provenance_concept_id in Provenance("EHR administration record"))
    filter(route_concept_id in Route("Intravenous"))
    define_era(datetime, dateadd_day(1, end_datetime))
    group(person_id, era)
    define(datetime => min(datetime), end_datetime => max(end_datetime))
end
```
"""
@funsql define_era(datetime, end_datetime; by = [person_id], name = era, before = nothing, after = nothing, private = false) = begin
    partition(
        by = $by,
        order_by = [$datetime],
        frame = (mode = rows, start = -Inf, finish = -1),
        name = _define_era_preceding)
    define(
        _define_era_start =>
            _define_era_preceding.max($end_datetime) >= $datetime ? 0 : 1)
    partition(
        by = $by,
        order_by = [$datetime, _define_era_start.desc()],
        frame = (mode = rows, start = -Inf, finish = 0),
        name = _define_era_preceding_or_current)
    define(
        $name => _define_era_preceding_or_current.sum(_define_era_start),
        before = $before,
        after = $after,
        private = $private)
    undefine(
        _define_era_preceding,
        _define_era_start,
        _define_era_preceding_or_current)
end

export funsql_define_era


"""
    @funsql care_site()
    @funsql care_site(concept_set)

Return records from the `CARE_SITE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `CARE_SITE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql care_site(Care_Site("Emergency Medicine"))
```
"""
function funsql_care_site
end

@funsql care_site() = begin
    from(care_site)
    define(source_concept_id => care_site_source_concept_id, after = care_site_source_concept_id)
    define(
        place_of_service_concept_id,
        location_id,
        care_site_source_concept_id,
        care_site_source_value,
        place_of_service_source_value,
        private = true)
end

@funsql care_site(concept_set) = begin
    care_site()
    filter(source_concept_id in $concept_set)
end

export funsql_care_site


"""
    @funsql provider()
    @funsql provider(concept_set)

Return records from the `PROVIDER` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `PROVIDER` columns are renamed or omitted from the output.

# Examples

```julia
@funsql provider(Provider("Rheumatology"))
```
"""
function funsql_provider
end

@funsql provider() = begin
    from(provider)
    define(concept_id => specialty_concept_id, after = specialty_concept_id)
    define(source_concept_id => specialty_source_concept_id, after = specialty_source_concept_id)
    define(
        specialty_concept_id,
        year_of_birth,
        provider_source_value,
        specialty_source_value,
        specialty_source_concept_id,
        private = true)
end

@funsql provider(concept_set) = begin
    provider()
    filter(concept_id in $concept_set)
end

export funsql_provider


"""
    @funsql person()

Return records from the `PERSON` table.  For clarity, some columns are renamed
or omitted from the output.

# Examples

```julia
@funsql begin
    person()
    filter(gender_concept_id in Gender("MALE"))
    filter(birth_datetime >= "2000-01-01")
end
```
"""
@funsql person() = begin
    from(person)
    define(
        location_id,
        care_site_id,
        person_source_value,
        gender_source_value,
        race_source_value,
        ethnicity_source_value,
        private = true)
end

export funsql_person


"""
    @funsql observation_period()

Return records from the `OBSERVATION_PERIOD` table.  For clarity, some columns
are renamed or omitted from the output.

# Examples

```julia
@funsql begin
    person()
    attach_earliest(observation_period => observation_period())
end
```
"""
@funsql observation_period() = begin
    from(observation_period)
    define(datetime => timestamp(observation_period_start_date), after = observation_period_start_date)
    define(end_datetime => timestamp(observation_period_end_date), after = observation_period_end_date)
    define(provenance_concept_id => period_type_concept_id, after = period_type_concept_id)
    define(
        observation_period_start_date,
        observation_period_end_date,
        period_type_concept_id,
        private = true)
end

export funsql_observation_period


"""
    @funsql visit()
    @funsql visit(concept_set)

Return records from the `VISIT_OCCURRENCE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `VISIT_OCCURRENCE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql visit(Visit("IP"))
```
"""
function funsql_visit
end

@funsql visit() = begin
    from(visit_occurrence)
    define(concept_id => visit_concept_id, after = visit_concept_id)
    define(datetime => visit_start_datetime, after = visit_start_datetime)
    define(end_datetime => visit_end_datetime, after = visit_end_datetime)
    define(provenance_concept_id => visit_type_concept_id, after = visit_type_concept_id)
    define(source_concept_id => visit_source_concept_id, after = visit_source_concept_id)
    define(
        visit_concept_id,
        visit_start_date,
        visit_start_datetime,
        visit_end_date,
        visit_end_datetime,
        visit_type_concept_id,
        visit_source_value,
        visit_source_concept_id,
        admitted_from_concept_id,
        admitted_from_source_value,
        discharged_to_concept_id,
        discharged_to_source_value,
        preceding_visit_occurrence_id,
        private = true)
    as(visit)
end

@funsql visit(concept_set) = begin
    visit()
    filter(concept_id in $concept_set)
end

export funsql_visit


"""
    @funsql condition()
    @funsql condition(concept_set)

Return records from the `CONDITION_OCCURRENCE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `CONDITION_OCCURRENCE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql condition(SNOMED("59621000", "Essential hypertension"))
```

```julia
@funsql condition(ICD10CM("I10", "Essential (primary) hypertension"))
```
"""
function funsql_condition
end

@funsql condition() = begin
    from(condition_occurrence)
    define(concept_id => condition_concept_id, after = condition_concept_id)
    define(datetime => condition_start_datetime, after = condition_start_datetime)
    define(end_datetime => condition_end_datetime, after = condition_end_datetime)
    define(provenance_concept_id => condition_type_concept_id, after = condition_type_concept_id)
    define(status_concept_id => condition_status_concept_id, after = condition_status_concept_id)
    define(source_concept_id => condition_source_concept_id, after = condition_source_concept_id)
    define(status_source_concept_id => condition_status_source_concept_id, after = condition_status_source_concept_id)
    define(
        condition_concept_id,
        condition_start_date,
        condition_start_datetime,
        condition_end_date,
        condition_end_datetime,
        condition_type_concept_id,
        condition_status_concept_id,
        stop_reason,
        visit_detail_id,
        condition_source_value,
        condition_source_concept_id,
        condition_status_source_value,
        condition_status_source_concept_id,
        private = true)
    as(condition)
end

@funsql condition(concept_set) = begin
    condition()
    filter(
        concept_id in $concept_set ||
        source_concept_id in $concept_set.map_to_related_concepts("edg_current_icd10.code of"))
end

export funsql_condition


"""
    @funsql drug()
    @funsql drug(concept_set)

Return records from the `DRUG_EXPOSURE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `DRUG_EXPOSURE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql drug(RxNorm("acetaminophen"))
```
"""
function funsql_drug
end

@funsql drug() = begin
    from(drug_exposure)
    define(concept_id => drug_concept_id, after = drug_concept_id)
    define(datetime => drug_exposure_start_datetime, after = drug_exposure_start_datetime)
    define(end_datetime => drug_exposure_end_datetime, after = drug_exposure_end_datetime)
    define(provenance_concept_id => drug_type_concept_id, after = drug_type_concept_id)
    define(mode_concept_id => drug_mode_concept_id, after = drug_mode_concept_id)
    define(source_concept_id => drug_source_concept_id, after = drug_source_concept_id)
    define(
        drug_concept_id,
        drug_exposure_start_date,
        drug_exposure_start_datetime,
        drug_exposure_end_date,
        drug_exposure_end_datetime,
        verbatim_end_date,
        drug_type_concept_id,
        drug_mode_concept_id,
        stop_reason,
        lot_number,
        visit_detail_id,
        drug_source_value,
        drug_source_concept_id,
        route_source_value,
        fill_unit_source_value,
        dose_unit_source_value,
        rate_unit_source_value,
        private = true)
    as(drug)
end

@funsql drug(concept_set) = begin
    drug()
    filter(
        concept_id in $concept_set ||
        source_concept_id in
            append(
                $concept_set.map_to_related_concepts("rxnorm_codes.rxnorm_code of"),
                $concept_set.map_to_related_concepts("rxnorm_codes.rxnorm_code of").map_to_related_concepts("rx_med_two.proxy_med_for_dc_id of")))
end

export funsql_drug


"""
    @funsql procedure()
    @funsql procedure(concept_set)

Return records from the `PROCEDURE_OCCURRENCE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `PROCEDURE_OCCURRENCE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql procedure(CPT4("1013012", "Electrocardiogram, routine ECG with at least 12 leads"))
```
"""
function funsql_procedure
end

@funsql procedure() = begin
    from(procedure_occurrence)
    define(concept_id => procedure_concept_id, after = procedure_concept_id)
    define(datetime => procedure_datetime, after = procedure_datetime)
    define(end_datetime => procedure_end_datetime, after = procedure_end_datetime)
    define(provenance_concept_id => procedure_type_concept_id, after = procedure_type_concept_id)
    define(source_concept_id => procedure_source_concept_id, after = procedure_source_concept_id)
    define(
        procedure_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_end_date,
        procedure_end_datetime,
        procedure_type_concept_id,
        modifier_concept_id,
        quantity,
        visit_detail_id,
        procedure_source_value,
        procedure_source_concept_id,
        modifier_source_value,
        private = true)
    as(procedure)
end

@funsql procedure(concept_set) = begin
    procedure()
    filter(
        concept_id in $concept_set ||
        source_concept_id in $concept_set)
end

export funsql_procedure


"""
    @funsql device()
    @funsql device(concept_set)

Return records from the `DEVICE_EXPOSURE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `DEVICE_EXPOSURE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql device(HCPCS("A4615", "Cannula, nasal"))
```
"""
function funsql_device
end

@funsql device() = begin
    from(device_exposure)
    define(concept_id => device_concept_id, after = device_concept_id)
    define(datetime => device_exposure_start_datetime, after = device_exposure_start_datetime)
    define(end_datetime => device_exposure_end_datetime, after = device_exposure_end_datetime)
    define(provenance_concept_id => device_type_concept_id, after = device_type_concept_id)
    define(source_concept_id => device_source_concept_id, after = device_source_concept_id)
    define(
        device_concept_id,
        device_exposure_start_date,
        device_exposure_start_datetime,
        device_exposure_end_date,
        device_exposure_end_datetime,
        device_type_concept_id,
        unique_device_id,
        production_id,
        quantity,
        visit_detail_id,
        device_source_value,
        device_source_concept_id,
        unit_concept_id,
        unit_source_value,
        unit_source_concept_id,
        private = true)
    as(device)
end

@funsql device(concept_set) = begin
    device()
    filter(
        concept_id in $concept_set ||
        source_concept_id in $concept_set)
end

export funsql_device


"""
    @funsql measurement()
    @funsql measurement(concept_set)

Return records from the `MEASUREMENT` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `MEASUREMENT` columns are renamed or omitted from the output.

# Examples

```julia
@funsql measurement(LOINC("8867-4", "Heart rate"))
```
"""
function funsql_measurement
end

@funsql measurement() = begin
    from(measurement)
    define(concept_id => measurement_concept_id, after = measurement_concept_id)
    define(datetime => measurement_datetime, after = measurement_datetime)
    define(provenance_concept_id => measurement_type_concept_id, after = measurement_type_concept_id)
    define(mode_concept_id => measurement_mode_concept_id, after = measurement_mode_concept_id)
    define(source_concept_id => measurement_source_concept_id, after = measurement_source_concept_id)
    define(
        measurement_concept_id,
        measurement_date,
        measurement_datetime,
        measurement_time,
        measurement_type_concept_id,
        measurement_mode_concept_id,
        visit_detail_id,
        measurement_source_value,
        measurement_source_concept_id,
        unit_source_concept_id,
        measurement_event_id,
        meas_event_field_concept_id,
        private = true)
end

@funsql measurement(concept_set) = begin
    measurement()
    filter(concept_id in $concept_set)
end

export funsql_measurement


"""
    @funsql observation()
    @funsql observation(concept_set)

Return records from the `OBSERVATION` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `OBSERVATION` columns are renamed or omitted from the output.

# Examples

```julia
@funsql observation(SNOMED("32911000", "Homeless"))
```

```julia
@funsql observation(ICD10CM("Z00.0", "Encounter for general adult medical examination"))
```
"""
function funsql_observation
end

@funsql observation() = begin
    from(observation)
    define(concept_id => observation_concept_id, after = observation_concept_id)
    define(datetime => observation_datetime, after = observation_datetime)
    define(provenance_concept_id => observation_type_concept_id, after = observation_type_concept_id)
    define(source_concept_id => observation_source_concept_id, after = observation_source_concept_id)
    define(
        observation_concept_id,
        observation_date,
        observation_datetime,
        observation_type_concept_id,
        value_as_number,
        value_as_string,
        qualifier_concept_id,
        unit_concept_id,
        visit_detail_id,
        observation_source_value,
        observation_source_concept_id,
        unit_source_value,
        qualifier_source_value,
        value_source_value,
        observation_event_id,
        obs_event_field_concept_id,
        private = true)
end

@funsql observation(concept_set) = begin
    observation()
    filter(
        concept_id in $concept_set ||
        source_concept_id in $concept_set.map_to_related_concepts("edg_current_icd10.code of"))
end

export funsql_observation


# Reexport those TRDW symbols that have not been overriden here.

_reexport() =
    for name in names(TRDW)
        !(name in names(@__MODULE__)) || continue
        @eval begin
            using ..TRDW: $name
            export $name
        end
    end

_reexport()

end
