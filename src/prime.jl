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
    as(omop)
    define(
        omop.concept_id,
        omop.concept_name,
        omop.vocabulary_id,
        omop.concept_code,
        omop.domain_id,
        omop.concept_class_id,
        omop.standard_concept,
        omop.invalid_reason)
end

@funsql concept(concept_ids...) = begin
    concept()
    filter(in(concept_id, $(concept_ids...)))
end

export funsql_concept


"""
    @funsql concept_like(code_or_name)
    @funsql concept_like(code, name)

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
        concept_like(\$code_or_name, \$name))
end

@funsql Gender("F")

@funsql Gender("Female")

@funsql Gender("F", "Female")

@funsql Gender("Fem…")
```
"""
function funsql_concept_like(code_or_name, name = nothing)
    name_pattern = replace(something(name, code_or_name), "…" => "%", "..." => "%")
    if name === nothing
        @funsql concept_code == $code_or_name || ilike(concept_name, $name_pattern)
    else
        @funsql concept_code == $code_or_name && ilike(concept_name, $name_pattern)
    end
end

export funsql_concept_like


"""
    @funsql concept_descend()

Add descendant concepts to the input concept set.

This combinator should be used only with standard or custom concepts since
any non-standard concepts in the input will be excluded from the result.

# Examples

```julia
@funsql Hypertension() = begin
    concept(316866 #= Hypertensive disorder =#)
    concept_descend()
end
```
"""
@funsql concept_descend() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.ancestor_concept_id)
    join(
        concept(),
        concept_ancestor.descendant_concept_id == concept_id,
        optional = true)
    define(concept_id => concept_ancestor.descendant_concept_id)
end

export funsql_concept_descend


"""
    @funsql concept_include(concept_set)

Add the given concept set to the input concept set.  With no input, return
the given concept set.  Commonly used for defining composable vocabulary
combinators.

# Examples

```julia
@funsql SNOMED(code_or_name, name = nothing) =
    concept_include(
        begin
            concept()
            filter(
                vocabulary_id == "SNOMED" &&
                concept_like(\$code_or_name, \$name))
            concept_descend()
        end)

@funsql begin
    SNOMED("195967001", "Asthma")
    SNOMED("233604007", "Pneumonia")
end
```
"""
@funsql concept_include(concept_set) =
    append($concept_set)

export funsql_concept_include


"""
    @funsql concept_exclude(concept_set)

Remove the given concept set from the input concept set.

# Examples

```julia
@funsql begin
    SNOMED("73211009", "Diabetes mellitus")
    concept_exclude(SNOMED("11687002", "Gestational diabetes mellitus"))
end
```
"""
@funsql concept_exclude(concept_set) = begin
    left_join(concept_exclude => $concept_set.group(concept_id), concept_id == concept_exclude.concept_id)
    filter(is_null(concept_exclude.concept_id))
end

export funsql_concept_exclude


"""
    @funsql concept_intersect(concept_set)

Restrict the input concept set to concepts that also appear in the given concept
set.

# Examples

```julia
@funsql ChronicBackPain() = begin
    SNOMED("82423001", "Chronic pain")
    concept_intersect(SNOMED("161891005", "Backache"))
end
```
"""
@funsql concept_intersect(concept_set) =
    join(concept_intersect => $concept_set.group(concept_id), concept_id == concept_intersect.concept_id)

export funsql_concept_intersect


"""
    @funsql concept_deduplicate()

Remove duplicate concept records from the input concept set.
"""
@funsql concept_deduplicate() = begin
    partition(name = concept_deduplicate, by = [concept_id], order_by = [missing])
    filter(concept_deduplicate.row_number() <= 1)
end

export funsql_concept_deduplicate


"""
    @funsql care_site()
    @funsql care_site(concept_set)

Return records from the `CARE_SITE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `CARE_SITE` columns are renamed or omitted from the output.

# Examples

```julia

@funsql care_site(CareSite("Emergency Medicine"))
```
"""
function funsql_care_site
end

@funsql care_site() = begin
    from(care_site)
    as(omop)
    define(
        omop.care_site_id,
        omop.care_site_name,
        source_concept_id => omop.care_site_source_concept_id,
        specialty_source_concept_id => omop.specialty_source_concept_id)
end

@funsql care_site(concept_set) = begin
    care_site()
    filter(source_concept_id in $concept_set)
end

export funsql_care_site


"""
    @funsql CareSite(code_or_name; descend = true)
    @funsql CareSite(code, name; descend = true)

Generate a concept set for care sites.

Arguments should refer to a concept within care site concept hierarchy,
which includes locations (sites), departments, and department specialties.

# Example

```julia
@funsql TMC() =
    CareSite("Tufts Medical Center Parent")

@funsql EmergencyDepartment() =
    CareSite("Emergency Medicine")
```
"""
@funsql CareSite(code_or_name, name = nothing; descend = true) =
    concept_include(
        begin
            concept()
            filter(
                assert_valid_concept(
                    in(vocabulary_id, "clarity_loc", "clarity_dep", "zc_specialty_dep") &&
                        concept_like($code_or_name, $name),
                    $(:(CareSite($code_or_name, $name; descend = $descend)))))
            switch($descend, concept_descend())
        end)

export funsql_CareSite


"""
    @funsql Gender(code, name)
    @funsql Gender(code_or_name)

Generate a concept set for the patient's sex.

Arguments should refer to a concept from the standard OMOP *Gender* vocabulary.
"""
@funsql Gender(code_or_name, name = nothing) = begin
    concept()
    filter(
        assert_valid_concept(
            vocabulary_id == "Gender" &&
            concept_like($code_or_name, $name)))
end

export funsql_Gender

"""
    @funsql Male()

Generate a concept set representing the *Male* sex.

# Examples

```julia
@funsql male() = begin
    person()
    filter(gender_concept_id in Male())
end
```
"""
@funsql Male() =
    concept(8507 #= MALE =#)

export funsql_Male

"""
    @funsql Female()

Generate a concept set representing the *Female* sex.

# Examples

```julia
@funsql female() = begin
    person()
    filter(gender_concept_id in Female())
end
```
"""
@funsql Female() =
    concept(8532 #= FEMALE =#)

export funsql_Female


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
